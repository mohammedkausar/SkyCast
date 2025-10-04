import json
import os

import pandas as pd

from src.ETL.load import LoadCities


pd.set_option('display.width', None)
def run_pipeline(config):
    cdc = config["CDC"]
    print("Staging the optimised type data in DB raw table")
    LoadCities(config).load_to_raw_staging(cdc)

def lambda_handler(event, context):
    try:
        base_for_lambda = os.path.dirname(os.path.abspath(__file__))
        file_for_lambda = json.load(open(os.path.join(base_for_lambda, "config", "config.json")))
        run_pipeline(file_for_lambda)
        return {"status": "success"}
    except Exception as e:
        print(f"Error while invoking lambda: {str(e)}")
        raise e
