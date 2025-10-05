import boto3
import json
import io


class S3Ops:
    def __init__(self,s3_config):
        self.s3_bucket_name = s3_config["S3"]["SKYCAST_BUCKET"]["skycast-weather-report"]
        self.s3 = boto3.client("s3")
    def get_s3_object(self,key):
        try:
            obj = self.s3.get_object(Bucket=self.s3_bucket_name,Key=key)
            return obj
        except self.s3.exceptions.NoSuchKey as e:
            print(f"Error while fetching from s3 bucket: {str(e)}")
            raise e

    def put_to_s3_object(self, key, obj, file_type='json'):
        try:


            if file_type == 'json':
                body = json.dumps(obj)
                content_type = "application/json"

            elif file_type == 'csv':
                csv_buffer = io.StringIO()
                obj.to_csv(csv_buffer, index=False)
                body = csv_buffer.getvalue()
                content_type = "text/csv"

            elif file_type == 'parquet':
                parquet_buffer = io.BytesIO()
                obj.to_parquet(parquet_buffer, index=False, engine='pyarrow')
                body = parquet_buffer.getvalue()
                content_type = "application/octet-stream"

            else:
                raise ValueError(f"Unsupported type: {file_type}")

            self.s3.put_object(
                Bucket=self.s3_bucket_name,
                Key=key,
                Body=body,
                ContentType=content_type
            )
        except Exception as e:
            print(f"S3 put object failed: {str(e)}")
            raise e
