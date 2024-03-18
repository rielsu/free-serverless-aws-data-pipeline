import awsgi
import os
import boto3
import csv
import gzip
import io
from flask import Flask, jsonify, request
from werkzeug.utils import secure_filename
import uuid

app = Flask(__name__)

# Get the bucket name from the environment variable
DATA_LAKE_BUCKET_NAME = os.getenv('DATA_LAKE_BUCKET_NAME')

athena_client = boto3.client('athena', region_name='us-east-2')

@app.route('/')
def index():
    return jsonify(status=200, message='Hello Flask!')

@app.route('/employee-hires')
def get_employee_hires():
    query = """
        SELECT department_id, job_id,
            COUNT(CASE WHEN EXTRACT(QUARTER FROM CAST(CASE WHEN datetime <> '' THEN from_iso8601_timestamp(datetime) END AS timestamp)) = 1 THEN 1 END) AS Q1,
            COUNT(CASE WHEN EXTRACT(QUARTER FROM CAST(CASE WHEN datetime <> '' THEN from_iso8601_timestamp(datetime) END AS timestamp)) = 2 THEN 1 END) AS Q2,
            COUNT(CASE WHEN EXTRACT(QUARTER FROM CAST(CASE WHEN datetime <> '' THEN from_iso8601_timestamp(datetime) END AS timestamp)) = 3 THEN 1 END) AS Q3,
            COUNT(CASE WHEN EXTRACT(QUARTER FROM CAST(CASE WHEN datetime <> '' THEN from_iso8601_timestamp(datetime) END AS timestamp)) = 4 THEN 1 END) AS Q4
        FROM hired_employees
        WHERE EXTRACT(YEAR FROM CAST(CASE WHEN datetime <> '' THEN from_iso8601_timestamp(datetime) END AS timestamp)) = 2021
        GROUP BY department_id, job_id
        ORDER BY department_id, job_id
    """
    database = 'dev-generic-data-catalog'
    output_location = 's3://generic-dev-athena-results/'

    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': output_location}
        )
        query_execution_id = response['QueryExecutionId']

        # Wait for the query to complete
        while True:
            response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = response['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break

        if status == 'SUCCEEDED':
            results = athena_client.get_query_results(QueryExecutionId=query_execution_id)

            # Process the results
            data = []
            for row in results['ResultSet']['Rows'][1:]:  # Skip the header row
                row_data = []
                for col in row['Data']:
                    row_data.append(col.get('VarCharValue', ''))
                data.append(row_data)

            # Extract column names
            columns = [col['VarCharValue'] for col in results['ResultSet']['Rows'][0]['Data']]

            # Convert to JSON
            json_data = [dict(zip(columns, row)) for row in data]
            return jsonify(json_data)
        else:
            raise Exception(f'Query failed with status {status}')
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify(status=400, message='No file part')
    file = request.files['file']
    if file.filename == '':
        return jsonify(status=400, message='No selected file')

    # Determine the file type (departments, jobs, employees)
    file_type = request.form.get('type')
    if file_type not in ['departments', 'jobs', 'employees']:
        return jsonify(status=400, message='Invalid file type')

    if file and file.filename.endswith('.csv'):
        filename = secure_filename(file.filename)

        # Read the contents of the SpooledTemporaryFile into a BytesIO object
        file_contents = io.BytesIO(file.read())

        # Wrap the BytesIO object in a TextIOWrapper for text mode
        text_file = io.TextIOWrapper(file_contents, encoding='utf-8')

        # Read the CSV file and split it into chunks
        reader = csv.reader(text_file)
        headers = next(reader)  # Get the headers from the first row

        # Generate a unique identifier for idempotency
        unique_id = str(uuid.uuid4())
        s3_client = boto3.client('s3')

        chunk_size = 1000
        chunk = []
        chunk_count = 0

        for row in reader:
            chunk.append(row)
            if len(chunk) == chunk_size:
                chunk_count += 1
                s3_key = f"{file_type}/{unique_id}/{filename}_part{chunk_count}.csv.gz"
                with io.BytesIO() as buffer:
                    with gzip.GzipFile(fileobj=buffer, mode='w') as gzip_file:
                        writer = csv.writer(io.TextIOWrapper(gzip_file, newline=''))
                        writer.writerow(headers)
                        writer.writerows(chunk)
                    buffer.seek(0)
                    s3_client.upload_fileobj(buffer, DATA_LAKE_BUCKET_NAME, s3_key)
                chunk = []  # Reset the chunk

        # Upload any remaining rows in the last chunk
        if chunk:
            chunk_count += 1
            s3_key = f"{file_type}/{unique_id}/{filename}_part{chunk_count}.csv.gz"
            with io.BytesIO() as buffer:
                with gzip.GzipFile(fileobj=buffer, mode='w') as gzip_file:
                    writer = csv.writer(io.TextIOWrapper(gzip_file, newline=''))
                    writer.writerow(headers)
                    writer.writerows(chunk)
                buffer.seek(0)
                s3_client.upload_fileobj(buffer, DATA_LAKE_BUCKET_NAME, s3_key)

        return jsonify(status=200, message='File uploaded successfully', file_parts=chunk_count)
    else:
        return jsonify(status=400, message='Invalid file format')

def handler(event, context):
    return awsgi.response(app, event, context)
