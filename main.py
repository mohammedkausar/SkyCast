import json
import os

from src.ETL.extract import ExtractCities
from src.ETL.transform import TransformCities
from src.ETL.stage import StageCities

# your helper to read config

def run_pipeline(config):
    ExtractCities(config).extract_data()
    transformed = TransformCities(config).transform_data()
    StageCities(config).upload_file_to_s3(transformed)

if __name__ == "__main__":
    base = os.path.dirname(os.path.abspath(__file__))
    file = json.load(open(os.path.join(base, "config", "config.json")))
    run_pipeline(file)
