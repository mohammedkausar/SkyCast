import io
import boto3
import pandas as pd
import json


class TransformCities:
    def __init__(self,config):
        self.bucket_name = config["S3"]["SKYCAST-BUCKET"]["NAME"]
        self.bucket_key = config["S3"]["SKYCAST-BUCKET"]["KEYS"][1]

    #private method to fetch the data from s3 bucket
    def _fetch_staged_data(self):
        try:
            s3= boto3.client("s3")
            staged_data = s3.get_object(Bucket =self.bucket_name,Key=self.bucket_key)
            staged_df = pd.read_parquet(io.BytesIO(staged_data["Body"].read()))
            return  staged_df
        except Exception as e:
            print(f"Error in fetchin requested data {str(e)}")

    #private method to flatten the nested data from data frame
    @staticmethod
    def _flatten_data_frame(df: pd.DataFrame)->pd.DataFrame:
        try:
            # print(df.head())
            print("Flattening started")
            df_json = json.loads(df.to_json(orient='records'))
            normalise_df= pd.json_normalize(df_json)
            # print(normalise_df)
            normalise_df.columns = normalise_df.columns.str.replace('.', '_')

            #Loop through the normalised dataframe columns and flatten nested lists
            for col in normalise_df.columns:
                if normalise_df[col].apply(lambda x: isinstance(x, list)).any():
                    normalise_df = normalise_df.explode(col, ignore_index=True)
                    expand_list = pd.json_normalize(normalise_df[col])
                    expand_list = expand_list.add_prefix(f"{col}_")
                    normalise_df = normalise_df.drop(columns=[col]).join(expand_list)
            # print(normalise_df)
            return normalise_df
        except Exception as e:
            print(f"Unable to flatten data: {str(e)}")
    @staticmethod
    def _convert_data_type(df: pd.DataFrame):
        print(df.head())


    #wrapper method to fetch and flatten the raw staged data
    def transform_data(self):
        try:
            data_to_transform = self._fetch_staged_data()
            transformed_data = self._flatten_data_frame(data_to_transform)
            # optimised_type_data = self._convert_data_type(transformed_data)
            return transformed_data
        except Exception as e:
            print(f"Unable to transform data: {str(e)}")