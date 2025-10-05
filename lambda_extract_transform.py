import json
import os

import pandas as pd

from src.ETL.extract import ExtractCities
from src.ETL.transform import TransformCities
from src.ETL.stage import StageCities
from utils.s3_ops import S3Ops

pd.set_option('display.width', None)

base = os.path.dirname(os.path.abspath(__file__))
file = json.load(open(os.path.join(base, "config", "config.json")))

def run_pipeline(config):
    try:
        print("Extraction started")
        extracted_data =ExtractCities(config).extract_data()
        StageCities(config,'raw').upload_file_to_s3(extracted_data)
        print("transformation started")
        transformed = TransformCities(config).transform_data()
        print("Staging the optimised type data in S3")
        cdc = max(transformed['dt'])
        StageCities(config,"complete",cdc).upload_file_to_s3(transformed)

        #Update the max date value of that particular fetch in JSON
        s3 = S3Ops(config)
        key =config["S3"]["SKYCAST-BUCKET"]["KEYS"][3]
        last_cdc = {"last_cdc": cdc}
        s3.put_to_s3_object(key,last_cdc,'json')
    except Exception as e:
        print(f"Error while running pipeline method: {str(e)}")

def lambda_handler(event, context):
    try:
        run_pipeline(file)
        return {"status": "success"}
    except Exception as e:
        print(f"Error while invoking lambda: {str(e)}")
        raise e
