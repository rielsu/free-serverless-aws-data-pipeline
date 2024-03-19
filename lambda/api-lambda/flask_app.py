from flask import Flask, jsonify, request
from file_upload import process_csv_file
from athena_query import execute_athena_query
from queries import (
    employees_hired_per_job_2021,
    over_mean_employees_hired_per_department,
)

app = Flask(__name__)

@app.route("/")
def index():
    return jsonify(status=200, message="Hello Flask!")

@app.route("/employee-hires")
def get_employee_hires():
    page_token = request.args.get('page_token', None)
    max_results = int(request.args.get('max_results', 50))
    return execute_athena_query(employees_hired_per_job_2021, next_token=page_token, max_results=max_results)

@app.route("/over-mean-employee-hires")
def get_over_mean_employee_hires():
    page_token = request.args.get('page_token', None)
    max_results = int(request.args.get('max_results', 50))
    return execute_athena_query(over_mean_employees_hired_per_department, next_token=page_token, max_results=max_results)

@app.route("/upload", methods=["POST"])
def upload_file():
    return process_csv_file(request)

if __name__ == "__main__":
    app.run(debug=True)
