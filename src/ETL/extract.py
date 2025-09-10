import os
import json
import io
import time

import boto3
import pandas as pd
import requests
from dotenv import load_dotenv
from pandas import DataFrame

load_dotenv()

class ExtractCities:
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    file_path = os.path.join(BASE_DIR, "config", "config.json")

    with open(file_path) as f:
        config = json.load(f)

    selected_cities = config["TOP_CITIES"]
    """
    constructor to initialize endpoint 
    """
    def __init__(self):
        self.base_url = self.config["BASE_URL"]
        self.weather_end_point = self.config["END_POINTS"]["WEATHER"]
        self.MAX_TRIES = 3
        self.RETRY_DELAY = 5

    @classmethod
    def city_from_parquet(cls):
        """Fetch parquet file from S3 and return as DataFrame"""
        try:
            s3 = boto3.client("s3")
            bucket = cls.config["S3"]["SKYCAST-BUCKET"]["NAME"]
            key = cls.config["S3"]["SKYCAST-BUCKET"]["KEY"]

            dim_city_obj = s3.get_object(Bucket=bucket, Key=key)
            df = pd.read_parquet(io.BytesIO(dim_city_obj["Body"].read()))
            return df

        except Exception as e:
            print(f"Error fetching parquet from S3: {e}")

    """
    Extract data from Openweather API with params
    """
    def extract_data(self, city_df: DataFrame):
        results=[]
        base_params = {
            "appid": os.getenv("OPENWEATHER_API_KEY"),
            "unit":"metric"
        }

        city_df = city_df[city_df["name"].isin(self.selected_cities)]
        for row in city_df.itertuples(index=False):
            attempt =0
            success = False
            lat = row.lat
            lon = row.lon
            params = base_params.copy()
            params.update({"lat":lat,"lon":lon})
            while attempt < self.MAX_TRIES and not success:
                try:
                    response = requests.get(f"{self.base_url}/{self.weather_end_point}",params=params)

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
        return  results