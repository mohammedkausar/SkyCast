from io import BytesIO
from pathlib import Path
import boto3
import pandas as pd


class StageCities:
    """
    Handles the staging (upload) of city data to AWS S3.

    Depending on the pipeline stage:
      - 'raw'  → uploads unprocessed/extracted data
      - 'complete' → uploads transformed and finalized data (tagged with CDC timestamp)
    """

    def __init__(self, config, status=None, cdc=""):
        """
        Initialize the StageCities class.

        Args:
            config (dict): Configuration dictionary containing S3 bucket and key details.
            status (str, optional): Indicates the stage type ('raw' or 'complete').
            cdc (str, optional): Change Data Capture (CDC) timestamp for uniquely tagging transformed data files.
        """
        self.bucket_name = config["S3"]["SKYCAST-BUCKET"]["NAME"]
        self.base_dir = Path(__file__).resolve().parents[2]

        # Determine which S3 key to use depending on the stage (raw or complete)
        if status == "complete":
            # Append the CDC timestamp to the final file name for version tracking
            self.key = config["S3"]["SKYCAST-BUCKET"]["KEYS"][2].split(".")
            self.key = f"_{cdc}.".join(self.key)
        else:
            # Use the raw stage key for unprocessed data uploads
            self.key = config["S3"]["SKYCAST-BUCKET"]["KEYS"][1]

    def upload_file_to_s3(self, df: pd.DataFrame):
        """
        Uploads a pandas DataFrame to the designated S3 bucket as a Parquet file.

        Steps:
        1. Convert the DataFrame into a Parquet file in memory.
        2. Upload the Parquet file to the specified S3 path.
        3. Print a success message upon completion.

        Args:
            df (pd.DataFrame): DataFrame to upload to S3.
        """
        try:
            # Create an S3 client
            s3 = boto3.client("s3")

            # Convert the DataFrame into a Parquet file stored in a memory buffer
            buffer = BytesIO()
            df.to_parquet(buffer, engine="pyarrow")  # Serialize DataFrame as Parquet
            buffer.seek(0)

            # Upload the file to the specified bucket and key
            s3.upload_fileobj(buffer, self.bucket_name, self.key)

            print(f"Successfully uploaded the file to S3 bucket: {self.bucket_name}/{self.key}")

        except Exception as e:
            # Log any upload errors
            print(f"Upload failed: {e}")
