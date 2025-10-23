import json
import os

from src.ETL.load import LoadCities


# Define the base directory and load the configuration file
base_for_lambda = os.path.dirname(os.path.abspath(__file__))
file_for_lambda = json.load(open(os.path.join(base_for_lambda, "config", "config.json")))


def lambda_handler(event,context):
    try:
        print('Starting lambda: Star schema loader')
        LoadCities(file_for_lambda).load_data_in_dim_fact_tables()
        return {
            "status": "SUCCESS",
            "message": "Fact and dimension tables updated successfully."
        }
    except Exception as e:
        error_message = f"Error while invoking lambda star schema load: {str(e)}"
        print(error_message)
        return {
            "status": "FAILED",
            "error": error_message
        }