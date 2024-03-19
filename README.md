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