from io import BytesIO
from pathlib import Path
import boto3
import pandas as pd


class StageCities:
    def __init__(self, config, status=None, cdc=""):
        self.bucket_name = config["S3"]["SKYCAST-BUCKET"]["NAME"]
        self.base_dir = Path(__file__).resolve().parents[2]

        if status == "complete":
            self.key = config["S3"]["SKYCAST-BUCKET"]["KEYS"][2].split(".")
            self.key = f"_{cdc}.".join(self.key)
        else:
            self.key = config["S3"]["SKYCAST-BUCKET"]["KEYS"][1]

    def upload_file_to_s3(self, df: pd.DataFrame):
        try:
            s3 = boto3.client("s3")
            buffer = BytesIO()
            df.to_parquet(buffer, engine="pyarrow") #type: ignore[arg-type]
            buffer.seek(0)
            s3.upload_fileobj(buffer, self.bucket_name, self.key)
            print(f"Successfully uploaded the raw file to s3 bucket")
        except Exception as e:
            print(f"Upload failed: {e}")
