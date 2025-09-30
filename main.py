import json
import os

import pandas as pd

from src.ETL.extract import ExtractCities
from src.ETL.transform import TransformCities
from src.ETL.stage import StageCities

# your helper to read config
pd.set_option('display.width', None)
def run_pipeline(config):
    print("Extraction started")
    extracted_data =ExtractCities(config).extract_data()
    StageCities(config,'raw').upload_file_to_s3(extracted_data)
    print("transformation started")
    transformed = TransformCities(config).transform_data()
    print(transformed)
    print("Staging the optimised type data in S3")
    cdc = max(transformed['dt'])
    # StageCities(config,"complete",cdc).upload_file_to_s3(transformed)

if __name__ == "__main__":
    base = os.path.dirname(os.path.abspath(__file__))
    file = json.load(open(os.path.join(base, "config", "config.json")))
    run_pipeline(file)
