import boto3
from flask import jsonify
import os

athena_client = boto3.client("athena", region_name="us-east-2")
DATABASE = os.getenv("DATABASE")
OUTPUT_LOCATION = os.getenv("OUTPUT_LOCATION")

def execute_athena_query(query, next_token=None, max_results=100):
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": DATABASE},
            ResultConfiguration={"OutputLocation": OUTPUT_LOCATION},
        )
        query_execution_id = response["QueryExecutionId"]

        # Wait for the query to complete
        while True:
            response = athena_client.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            status = response["QueryExecution"]["Status"]["State"]
            if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                break

        if status == "SUCCEEDED":
            # Use NextToken only if it's not None
            query_results_params = {
                "QueryExecutionId": query_execution_id,
                "MaxResults": max_results
            }
            if next_token:
                query_results_params["NextToken"] = next_token

            results = athena_client.get_query_results(**query_results_params)

            # Process the results
            data = []
            for row in results["ResultSet"]["Rows"][1:]:  # Skip the header row
                row_data = []
                for col in row["Data"]:
                    row_data.append(col.get("VarCharValue", ""))
                data.append(row_data)

            # Extract column names
            columns = [
                col["VarCharValue"] for col in results["ResultSet"]["Rows"][0]["Data"]
            ]

            # Convert to JSON
            json_data = {
                "data": [dict(zip(columns, row)) for row in data],
                "next_token": results.get("NextToken")
            }
            return jsonify(json_data)
        else:
            raise Exception(f"Query failed with status {status}")
    except Exception as e:
        return jsonify({"error": str(e)}), 500
