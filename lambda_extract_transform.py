import json
import os
import pandas as pd

from src.ETL.extract import ExtractCities
from src.ETL.transform import TransformCities
from src.ETL.stage import StageCities
from utils.s3_ops import S3Ops

pd.set_option('display.width', None)

# Base directory of the current file
base = os.path.dirname(os.path.abspath(__file__))

# Load configuration file
file = json.load(open(os.path.join(base, "config", "config.json")))


def run_pipeline(config):
    """
    Runs the end-to-end ETL pipeline for city data.

    Steps:
    1. Extract data from the source.
    2. Stage (upload) raw data to S3.
    3. Transform the data.
    4. Stage transformed data back to S3.
    5. Update the last CDC (change data capture) date in S3.
    """
    try:
        # Step 1: Extract data from the source
        print("Extraction started")
        extracted_data = ExtractCities(config).extract_data()

        # Step 2: Stage raw extracted data to S3 for backup/audit
        StageCities(config, 'raw').upload_file_to_s3(extracted_data)

        # Step 3: Transform the extracted data (type conversions, cleaning, enrichment, etc.)
        print("Transformation started")
        transformed = TransformCities(config).transform_data()

        # Step 4: Stage the transformed data to S3 under 'complete' folder with CDC timestamp
        print("Staging the optimised type data in S3")
        cdc = max(transformed['dt'])
        StageCities(config, "complete", cdc).upload_file_to_s3(transformed)

        # Step 5: Update the last processed CDC date in S3 (used for incremental loads)
        s3 = S3Ops(config)
        key = config["S3"]["SKYCAST-BUCKET"]["KEYS"][3]
        last_cdc = {"last_cdc": cdc}
        s3.put_to_s3_object(key, last_cdc, 'json')

    except Exception as e:
        # Catch and log any pipeline errors
        print(f"Error while running pipeline method: {str(e)}")


def lambda_handler(event, context):
    """
    AWS Lambda handler function that triggers the ETL pipeline.
    """
    try:
        # Trigger the ETL pipeline execution
        run_pipeline(file)
        return {"status": "success"}

    except Exception as e:
        # Log and re-raise the exception for AWS Lambda error tracking
        print(f"Error while invoking lambda: {str(e)}")
        raise e
