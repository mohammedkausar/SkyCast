import os
import json
import io
import time

import boto3
import pandas as pd
import requests
from dotenv import load_dotenv
from pandas import DataFrame

from src.pre_process import file_path, BASE_DIR

load_dotenv()

class ExtractCities:
    """
    constructor to initialize endpoint 
    """
    def __init__(self,config):
        self.base_url = config["BASE_URL"]
        self.weather_end_point = config["END_POINTS"]["WEATHER"]
        self.api_v = config["URL_VERSION"]["WEATHER_V"]
        self.MAX_TRIES = config["RETRY"]["MAX_TRIES"]
        self.RETRY_DELAY = config["RETRY"]["RETRY_DELAY"]
        self.selected_cities = config["TOP_CITIES"]
        self.bucket_name = config["S3"]["SKYCAST-BUCKET"]["NAME"]
        self.key_name = config["S3"]["SKYCAST-BUCKET"]["KEYS"][0]

    @staticmethod
    def _city_from_parquet(bucket_name,key_name):
        """Fetch parquet file from S3 and return as DataFrame"""
        try:
            s3 = boto3.client("s3")
            dim_city_obj = s3.get_object(Bucket=bucket_name, Key=key_name)
            df = pd.read_parquet(io.BytesIO(dim_city_obj["Body"].read()))
            return df

        except Exception as e:
            print(f"Error fetching parquet from S3: {e}")

    """Extract data from Openweather API with params"""
    def extract_data(self):
        city_df = self._city_from_parquet(self.bucket_name,self.key_name)
        results=[]
        base_params = {
            "appid": os.getenv("OPENWEATHER_API_KEY"),
            "units":"metric"
        }

        city_df = city_df[city_df["name"].str.lower().fillna("").isin(map(str.lower,self.selected_cities))]
        for row in city_df.itertuples(index=False):
            attempt =0
            success = False
            lat = row.lat
            lon = row.lon
            params = base_params.copy()
            params.update({"lat":lat,"lon":lon})
            while attempt < self.MAX_TRIES and not success:
                try:
                    response = requests.get(f"{self.base_url}/{self.api_v}/{self.weather_end_point}",params=params)
                    weather_data = response.json()
                    results.append(weather_data)
                    success=True
                except Exception as e:
                    attempt +=1
                    print(f"Attempt {attempt} failed for {row.id} : {str(e)}")
                    if attempt < self.MAX_TRIES:
                        time.sleep(self.RETRY_DELAY)
                    else:
                        print(f"Failed to fetch data for {row.id} after {self.MAX_TRIES} attempts" )
        return results



