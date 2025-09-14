from pathlib import Path
import boto3
import pandas as pd


class StageCities:
    def __init__(self,config):
        self.bucket_name = config["S3"]["SKYCAST-BUCKET"]["NAME"]
        self.key_name = config["S3"]["SKYCAST-BUCKET"]["KEYS"][1]
        self.base_dir = Path(__file__).resolve().parents[2]
        self.local_staging_copy_path = Path(self.base_dir) / "staging" / "raw" / "raw_weather_data.parquet"
        self.local_staging_copy_path.parent.mkdir(parents=True, exist_ok=True)

    #uploading the local staged file to the s3 bucket
    def upload_file_to_s3(self,df: pd.DataFrame):
        try:
            df.to_parquet(self.local_staging_copy_path, engine='pyarrow')
            s3 = boto3.client("s3")
            s3.upload_file(str(self.local_staging_copy_path),self.bucket_name,self.key_name)
        except Exception as e:
            print(f"Error uploading file {str(e)}")