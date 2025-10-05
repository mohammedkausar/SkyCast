import json
import os


import pandas as pd

from src.ETL.load import LoadCities
from utils.s3_ops import S3Ops

base_for_lambda = os.path.dirname(os.path.abspath(__file__))
file_for_lambda = json.load(open(os.path.join(base_for_lambda, "config", "config.json")))
pd.set_option('display.width', None)
def run_pipeline(config):
    try:
        s3 = S3Ops(config)
        key = config["S3"]["SKYCAST-BUCKET"]["KEYS"][3]
        obj = s3.get_s3_object(key)
        body = obj["Body"].read()
        if body:
            cdc_json = json.loads(body.decode('utf-8'))
            print("Staging the optimised type data in DB raw table")
            LoadCities(config).load_to_raw_staging(cdc_json['last_cdc'])
    except Exception as e:
        print(f"Error while fetching cdc: {str(e)}")

def lambda_handler(event, context):
    try:
        run_pipeline(file_for_lambda)
        return {"status": "success"}
    except Exception as e:
        print(f"Error while invoking lambda: {str(e)}")
        raise e
