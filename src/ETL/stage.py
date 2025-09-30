from io import BytesIO
from pathlib import Path
import boto3
import pandas as pd


class StageCities:
    def __init__(self, config, status=None, cdc=""):
        self.bucket_name = config["S3"]["SKYCAST-BUCKET"]["NAME"]
        self.base_dir = Path(__file__).resolve().parents[2]
        self.status = status
        self.file_name=""
        self.local_path = ""
        if status == "raw":
            self.key = config["S3"]["SKYCAST-BUCKET"]["KEYS"][1]
            self.file_name = "raw_weather_data.parquet"
            self.local_path = self.base_dir / "staging" / "raw" / self.file_name
            self.local_path.parent.mkdir(parents=True, exist_ok=True)

        elif status == "complete":
            self.key = config["S3"]["SKYCAST-BUCKET"]["KEYS"][2].split(".")
            self.key = f"_{cdc}.".join(self.key)

    def upload_file_to_s3(self, df: pd.DataFrame):
        try:
            s3 = boto3.client("s3")

            if self.status == "complete":
                buffer = BytesIO()
                df.to_parquet(buffer, engine="pyarrow") #type: ignore[arg-type]
                buffer.seek(0)
                s3.upload_fileobj(buffer, self.bucket_name, self.key)
            else:
                df.to_parquet(self.local_path, engine="pyarrow")
                s3.upload_file(str(self.local_path), self.bucket_name, self.key)

        except Exception as e:
            print(f"Upload failed: {e}")
