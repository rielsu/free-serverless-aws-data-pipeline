## Application Architecture

This application is a serverless data processing pipeline built on AWS. It uses a Flask application running in an AWS Lambda function, which is exposed via an API Gateway. The data is stored in batches in an S3 bucket, which serves as a data lake. AWS Glue is used to create a data catalog and external tables, and Amazon Athena is used to query the data.

### Main Components

- **Flask Application**: The core of the application is a Flask web server, which provides several API endpoints for interacting with the data. This application is packaged into a Docker image and run in an AWS Lambda function.

- **AWS Lambda**: The Flask application is hosted in an AWS Lambda function. This allows the application to scale automatically in response to incoming traffic.

- **API Gateway**: The AWS Lambda function is exposed via an API Gateway. This provides a single, unified API endpoint for clients to interact with.

- **S3 Data Lake**: Incoming data is stored in an S3 bucket, which serves as a data lake. The data is stored in batches for efficient processing.

- **AWS Glue Data Catalog**: AWS Glue is used to create a data catalog, which provides a unified view of the data in the data lake. AWS Glue also creates external tables, which allow the data in the S3 bucket to be queried as if it were in a traditional database.

- **Amazon Athena**: Amazon Athena is used to run SQL queries on the data in the S3 bucket. This allows for powerful, flexible analysis of the data.

### Data Flow

1. Data is sent to the Flask application via the API Gateway.
2. The Flask application processes the data and stores it in batches in the S3 bucket.
3. AWS Glue scans the S3 bucket and updates the data catalog and external tables.
4. Amazon Athena queries the data in the S3 bucket using the external tables created by AWS Glue.

# AWS CDK TypeScript Project

This is a project for managing AWS resources using the AWS Cloud Development Kit (CDK) and TypeScript.

## Project Structure

- `cdk.ts`: This is the main file where the CDK app is initialized and the stacks are defined.
- `lib/`: This directory contains the stack definitions.
  - `config-builder.ts`: This file contains the function to get the configuration for the current stage.
  - `data-stack.ts`: This file defines the `DataStack` stack.
  - `api-stack.ts`: This file defines the `genericApiStack` stack.
  - `data-catalog-stack.ts`: This file defines the `DataCatalogStack` stack.

## Getting Started

1. Install the necessary dependencies:

```bash
npm install
npm run build
cdk deploy -c stage=dev  --all
```

# Flask Application for Employee Data Analysis

This is a Flask application that provides APIs for uploading and analyzing employee data.

## Application Architecture

The application is structured around a Flask web server, which provides several API endpoints for interacting with the data.

### Main Components

- `flask_app.py`: This is the main file where the Flask app is initialized and the routes are defined.
- `file_upload.py`: This module contains the `process_csv_file` function, which is used to process uploaded CSV files.
- `athena_query.py`: This module contains the `execute_athena_query` function, which is used to execute queries on Amazon Athena.
- `queries.py`: This module contains the SQL queries that are used to analyze the data.

### API Endpoints

- `GET /`: Returns a welcome message.
- `GET /employee-hires`: Returns the number of employees hired per job in 2021. Supports pagination through `page_token` and `max_results` query parameters.
- `GET /over-mean-employee-hires`: Returns the departments that hired more employees than the average. Supports pagination through `page_token` and `max_results` query parameters.
- `POST /upload`: Accepts a CSV file upload and processes it.

## Data Handling

### Null Data

In our application, we take special care to handle null data. During the CSV file processing, any null values are identified and handled appropriately to ensure they do not cause errors during the analysis. Depending on the specific requirements of the analysis, null values may be ignored, replaced with a default value, or used to exclude the corresponding record from the analysis.

### Datetime Data

Datetime data is also carefully managed in our application. All datetime data is standardized to a common format (ISO 8601) during the CSV file processing. This ensures that the datetime data is consistent and can be correctly sorted and compared during the analysis.

## Idempotency

Idempotency is a key concept in the design of reliable APIs. An operation is said to be idempotent if making multiple identical requests has the same effect as making a single request.

In our application, the `POST /upload` endpoint is designed to be idempotent. If you upload the same CSV file multiple times, the system will recognize that the data has already been processed and will not duplicate it. This ensures that the system remains in a consistent state even if the upload operation is repeated.


## Testing the API with Postman

Postman is a popular tool for testing APIs. Here's how you can use it to test the `https://yqpmajftzf.execute-api.us-east-2.amazonaws.com/prod/upload` endpoint:

1. Download and install Postman from [https://www.postman.com/downloads/](https://www.postman.com/downloads/).

2. Open Postman and click on the `+` button to create a new tab.

3. In the new tab, select `POST` from the dropdown list of HTTP methods.

4. Enter the URL `https://yqpmajftzf.execute-api.us-east-2.amazonaws.com/prod/upload` into the URL field.

5. Below the URL field, click on the `Body` tab.

6. Select the `form-data` option.

7. In the `Key` field, enter `file`. In the `Value` field, click on the `Choose Files` button and select the CSV file you want to upload.

8. Click on the `Send` button to send the request.

You should see the response from the server in the lower part of the window. If the file was uploaded successfully, you should see a success message in the response.


## CSV File Formats

The application expects CSV files to be in specific formats depending on the type of data they contain. The expected formats are defined in the `file_type_mapping` dictionary in the `file_upload.py` file:

- `hired_employees`: This file should contain columns for `id`, `name`, `datetime`, `department_id`, and `job_id`. The `id`, `department_id`, and `job_id` should be integers, and `name` should be a string. The `datetime` should be a string in the format `%Y-%m-%dT%H:%M:%SZ`. Missing `job_id` values will be replaced with 0.

- `departments`: This file should contain columns for `id` and `department`. The `id` should be an integer and `department` should be a string.

- `jobs`: This file should contain columns for `id` and `job`. The `id` should be an integer and `job` should be a string.

Please ensure that your CSV files conform to these formats before uploading.

## Querying the Data

Once the data has been uploaded and processed, you can query it using the `/employee-hires` and `/over-mean-employee-hires` endpoints. Both endpoints support pagination.

### Employee Hires Endpoint

The `/employee-hires` endpoint returns a list of employees hired per job in 2021. You can paginate the results using the following query parameters:

- `page_token`: The token for the next page of results.
- `max_results`: The maximum number of results to return (default is 50).

The URL structure is as follows:

https://yqpmajftzf.execute-api.us-east-2.amazonaws.com/prod/employee-hires?page_token=&max_results=

### Over Mean Employee Hires Endpoint

The `/over-mean-employee-hires` endpoint returns a list of departments that hired more employees than the average. You can paginate the results using the following query parameters:

- `page_token`: The token for the next page of results.
- `max_results`: The maximum number of results to return (default is 50).

The URL structure is as follows:

https://yqpmajftzf.execute-api.us-east-2.amazonaws.com/prod/over-mean-employee-hires?page_token=&max_results=

Please replace `<PAGE_TOKEN>` and `<MAX_RESULTS>` with the actual values you want to use.