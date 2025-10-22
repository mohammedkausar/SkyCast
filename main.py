import json
import os

import pandas as pd

from src.ETL.extract import ExtractCities
from src.ETL.transform import TransformCities
from src.ETL.stage import StageCities
from src.ETL.load import LoadCities

pd.set_option('display.width', None)


def run_pipeline(config):
    """
    Run the full ETL pipeline: extract, stage raw, transform, stage complete, and load to DB
    """
    print('Extraction started')
    extracted_data = ExtractCities(config).extract_data()
    StageCities(config, 'raw').upload_file_to_s3(extracted_data)

    print('Transformation started')
    transformed = TransformCities(config).transform_data()

    print('Staging the optimised type data in S3')
    cdc = max(transformed['dt'])
    StageCities(config, 'complete', cdc).upload_file_to_s3(transformed)

    print('Staging the optimised type data in DB raw table')
    LoadCities(config).load_to_raw_staging(cdc)


if __name__ == '__main__':
    """
    Run pipeline locally
    """
    base = os.path.dirname(os.path.abspath(__file__))
    file = json.load(open(os.path.join(base, 'config', 'config.json')))
    run_pipeline(file)


def lambda_handler(event, context):
    """
    AWS Lambda handler to trigger the ETL pipeline
    """
    try:
        base_for_lambda = os.path.dirname(os.path.abspath(__file__))
        file_for_lambda = json.load(open(os.path.join(base_for_lambda, 'config', 'config.json')))
        run_pipeline(file_for_lambda)
        return {'status': 'success'}
    except Exception as e:
        print(f'Error while invoking lambda: {str(e)}')
        raise e
