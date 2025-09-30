import os
import io
import time
from datetime import timedelta, timezone, datetime

import boto3
import pandas as pd
import requests
from dotenv import load_dotenv


load_dotenv()

class ExtractCities:
    """
    constructor to initialize endpoint 
    """
    def __init__(self,config):
        self.base_url = config["BASE_URL"]
        self.weather_end_point = config["END_POINTS"]["WEATHER"]
        self.api_v = config["URL_VERSION"]["WEATHER_V"]
        self.MAX_TRIES = config["SLEEP"]["MAX_TRIES"]
        self.RETRY_DELAY = config["SLEEP"]["RETRY_DELAY"]
        # self.selected_cities = config["TOP_CITIES"]
        self.bucket_name = config["S3"]["SKYCAST-BUCKET"]["NAME"]
        self.key_name = config["S3"]["SKYCAST-BUCKET"]["KEYS"][0]
        self.batch_size = config["BATCH_SIZE"]
        self.batch_delay = config["SLEEP"]["BATCH_DELAY"]
        self.fetched_at = config["COLUMNS"]["FETCHED_AT"]

    @staticmethod
    def _city_from_parquet(bucket_name,key_name):
        """Fetch parquet file from S3 and return as DataFrame"""
        try:
            s3 = boto3.client("s3")
            dim_city_obj = s3.get_object(Bucket=bucket_name, Key=key_name)
            df = pd.read_parquet(io.BytesIO(dim_city_obj["Body"].read()))
            return df.head(10)

        except Exception as e:
            print(f"Error fetching parquet from S3: {e}")


    def extract_data(self):
        batch_weather_data =[]
        city_df = self._city_from_parquet(self.bucket_name,self.key_name)
        city_list= city_df.to_dict(orient="records")
        for i in range(0,len(city_list),self.batch_delay):
            batch =city_list[i:i+self.batch_delay]
            for city in batch:
                data=self._fetch_weather(city)
                batch_weather_data.append(data)
        if batch_weather_data:
            batch_weather_data=pd.DataFrame(batch_weather_data)
            return batch_weather_data
        return None
        #city_df = city_df[city_df["name"].str.lower().fillna("").isin(map(str.lower,self.selected_cities))]




    def _fetch_weather(self,city):
        """Extract data from Openweather API with params"""
        results=[]
        base_params = {
            "appid": os.getenv("OPENWEATHER_API_KEY"),
            "units": "metric"
        }
        attempt = 0
        success = False
        lat = city["lat"]
        lon = city["lon"]
        params = base_params.copy()
        params.update({"lat": lat, "lon": lon})
        while attempt < self.MAX_TRIES and not success:
            try:
                response = requests.get(f"{self.base_url}/{self.api_v}/{self.weather_end_point}",params=params)
                weather_data = response.json()
                time_zone = timezone(timedelta(hours=5,minutes=30))
                weather_data[self.fetched_at] = datetime.now(time_zone).strftime("%Y-%m-%d %H:%M:%S")
                success=True
                return weather_data
            except Exception as e:
                attempt +=1
                print(f"Attempt {attempt} failed for {city["id"]} : {str(e)}")
                if attempt < self.MAX_TRIES:
                    time.sleep(self.RETRY_DELAY)
                else:
                    print(f"Failed to fetch data for {city["id"]} after {self.MAX_TRIES} attempts" )
        return None

