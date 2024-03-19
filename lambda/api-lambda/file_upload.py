import boto3
import pandas as pd
from flask import jsonify
from werkzeug.utils import secure_filename
import uuid
import os
import io
import logging

DATA_LAKE_BUCKET_NAME = os.getenv("DATA_LAKE_BUCKET_NAME")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))

# Define a mapping of file types to column names and data types
file_type_mapping = {
    "hired_employees": {
        "columns": ["id", "name", "datetime", "department_id", "job_id"],
        "dtypes": {
            "id": "Int64",
            "name": "str",
            "datetime": "str",
            "department_id": "Int64",
            "job_id": "Int64",
        },
        "parse_dates": ["datetime"],
        "missing_values": {"job_id": 0},
        "date_formats": {"datetime": "%Y-%m-%dT%H:%M:%SZ"},
    },
    "departments": {
        "columns": ["id", "department"],
        "dtypes": {"id": "Int64", "department": "str"},
        "missing_values": {},
        "date_formats": {},
    },
    "jobs": {
        "columns": ["id", "job"],
        "dtypes": {"id": "Int64", "job": "str"},
        "missing_values": {},
        "date_formats": {},
    },
}


def process_csv_file(request):
    if "file" not in request.files:
        return jsonify(status=400, message="No file part")
    file = request.files["file"]

    if file.filename == "":
        return jsonify(status=400, message="No selected file")

    # Determine the file type (departments, jobs, employees)
    file_type = request.form.get("type")
    if file_type not in file_type_mapping:
        return jsonify(status=400, message="Invalid file type")

    if file and file.filename.endswith(".csv"):
        filename = secure_filename(file.filename)

        # Read the CSV file into a Pandas DataFrame with proper data types
        df = pd.read_csv(file.stream, dtype=file_type_mapping[file_type]["dtypes"])

        # Ensure the DataFrame has the correct columns
        num_chunks = process_dataframe(df, file_type, filename)

        return jsonify(
            status=200, message="File uploaded successfully", num_chunks=num_chunks
        )
    else:
        return jsonify(status=400, message="Invalid file format")


def process_dataframe(df, file_type, filename):
    # Handle missing values
    for column, default_value in file_type_mapping[file_type]["missing_values"].items():
        if column in df.columns:
            df[column] = df[column].fillna(default_value)

    # Ensure datetime columns are in the specified format
    for column, date_format in file_type_mapping[file_type]["date_formats"].items():
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], format=date_format, errors="coerce")

    # Upload the DataFrame to S3
    num_chunks = upload_dataframe_to_s3(df, file_type, filename)

    return num_chunks


def upload_dataframe_to_s3(df, file_type, filename):
    logging.info(f"Uploading {file_type} data to S3...")
    unique_id = str(uuid.uuid4())
    s3_client = boto3.client('s3')

    chunk_size = BATCH_SIZE
    num_chunks = (len(df) // chunk_size) + 1
    for i in range(num_chunks):
        try:
            chunk = df[i * chunk_size:(i + 1) * chunk_size]
            s3_key = f"{file_type}/{unique_id}/{filename}_part{i + 1}.csv.gz"
            with io.BytesIO() as buffer:
                chunk.to_csv(buffer, index=False, compression='gzip')
                buffer.seek(0)
                s3_client.upload_fileobj(buffer, DATA_LAKE_BUCKET_NAME, s3_key)
            logging.info(f"Uploaded chunk {i + 1} of {num_chunks} to {s3_key}")
        except Exception as e:
            logging.error(f"Error uploading chunk {i + 1} to S3: {e}")
            raise

    return num_chunks
