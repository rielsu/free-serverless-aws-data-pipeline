import boto3
import csv
import gzip
import io
from flask import jsonify
from werkzeug.utils import secure_filename
import uuid
import os

DATA_LAKE_BUCKET_NAME = os.getenv('DATA_LAKE_BUCKET_NAME')

def upload_file_to_s3(request):
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