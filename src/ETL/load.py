import io

import boto3
import pandas as pd
import psycopg2
from utils.data_cleaner import CleanData
from utils import db_connect

class LoadCities:
    def __init__(self,config):
        self.bucket_name = config["S3"]["SKYCAST-BUCKET"]["NAME"]
        self.bucket_key = config["S3"]["SKYCAST-BUCKET"]["KEYS"][2]
        self.cfg = db_connect.get_config()
        self.cdc = None
        self.schema = config["COLUMNS"]["TO_SQL_COLUMNS"]
        self.raw_table = config["TABLES"]["WEATHER_RAW"]

    def _fetch_raw_data_from_parquet(self):
        if self.cdc is None:
            raise ValueError("cdc must be set before fetching parquet data")

        try:
            split_bucket_key = self.bucket_key.split(".")
            bucket_key_cdc = f"_{self.cdc}.".join(split_bucket_key)
            s3 = boto3.client("s3")
            weather_data = s3.get_object(Bucket=self.bucket_name,Key=bucket_key_cdc)
            weather_df = pd.read_parquet(io.BytesIO(weather_data["Body"].read()))
            cleaned_weather_df = CleanData.sanitize_data_frame(weather_df)

            return cleaned_weather_df
        except Exception as e:
            print(f"Error while fetching data from raw data bucket: {str(e)}")
            raise e

    def load_to_raw_staging(self,cdc):
        try:
            self.cdc = cdc
            data_to_load = self._fetch_raw_data_from_parquet()
            if data_to_load is not None and not data_to_load.empty:
                cols = list(data_to_load.columns)
                buffer = io.StringIO()
                data_to_load.to_csv(buffer, index=False, header=False)
                buffer.seek(0)
                with psycopg2.connect(**self.cfg) as conn:
                    with conn.cursor() as cur:
                        cur.copy_from(buffer, table=self.raw_table, sep=",", columns=cols)
        except Exception as e:
            print(f"Error while loading data to raw table: {str(e)}")
            raise e
