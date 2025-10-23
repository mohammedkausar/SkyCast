import json
import os

import boto3
import pandas as pd

from src.ETL.load import LoadCities
from utils.s3_ops import S3Ops

# Define the base directory and load the configuration file
base_for_lambda = os.path.dirname(os.path.abspath(__file__))
file_for_lambda = json.load(open(os.path.join(base_for_lambda, "config", "config.json")))

# Set display options for pandas (for debugging/logging clarity)
pd.set_option('display.width', None)


def run_pipeline(config):
    """
    Runs the Load stage of the ETL pipeline.

    Steps:
    1. Connect to S3 and fetch the latest CDC (change data capture) JSON file.
    2. Parse the CDC value to determine which batch of data to load.
    3. Load the corresponding transformed data from S3 into the database raw staging table.
    """
    try:
        # Step 1: Initialize S3 client
        s3 = S3Ops(config)

        # Step 2: Get the key of the last CDC record file from configuration
        key = config["S3"]["SKYCAST-BUCKET"]["KEYS"][3]

        # Step 3: Retrieve the CDC JSON object from S3
        obj = s3.get_s3_object(key)
        body = obj["Body"].read()

        # Step 4: If the CDC file exists, load the corresponding dataset to raw table
        if body:
            cdc_json = json.loads(body.decode('utf-8'))
            print("Staging the optimised type data in DB raw table")
            LoadCities(config).load_to_raw_staging(cdc_json['last_cdc'])

    except Exception as e:
        # Log any exception that occurs during the load process
        print(f"Error while fetching cdc: {str(e)}")


def lambda_handler(event, context):
    """
    AWS Lambda handler function that triggers the load stage of the ETL pipeline.
    """
    event_client = boto3.client('events')
    try:
        # Execute the pipeline using the loaded configuration
        run_pipeline(file_for_lambda)
        response = {
            "status": "success",
            "message": "Raw data successfully loaded into raw_weather table.",
            "next_step": "LOAD_STAR_SCHEMA"
        }


        event_client.put_events(
            Entries=[
                {
                    "Source": "skycast_load",
                    "DetailType": "lambda_load_status",
                    "Detail" : json.dumps({
                        "status" : "success",
                        "next_step": response["next_step"]
                    }),
                    "EventBusName": "default"
                }
            ]
        )
        return  response

    except Exception as e:
        # Log and re-raise the exception for AWS Lambda monitoring
        error_message = f"Error while running raw data load: {str(e)}"
        print(error_message)


        event_client.put_events(
            Entries=[
                {
                    "Source": "skycast_load",
                    "DetailType": "lambda_load_status",
                    "Detail": json.dumps({
                        "status": "failure",
                        "error": error_message
                    }),
                    "EventBusName": "default"
                }
            ]
        )
        # Step Function will treat this as a failure
        return {
            "status": "FAILED",
            "error": error_message
        }
