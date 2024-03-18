from flask import Flask, jsonify, request
from file_upload import upload_file_to_s3
from athena_query import execute_athena_query

app = Flask(__name__)

@app.route('/')
def index():
    return jsonify(status=200, message='Hello Flask!')

@app.route('/employee-hires')
def get_employee_hires():
    # Call the function to execute the Athena query
    return execute_athena_query()

@app.route('/upload', methods=['POST'])
def upload_file():
    # Call the function to upload the file to S3
    return upload_file_to_s3(request)
