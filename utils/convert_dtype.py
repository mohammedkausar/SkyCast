import pandas as pd
from pandas import DataFrame


class DtypeConversion:
    def type_convert(self,df:DataFrame,schema:dict):
        pd.set_option('display.width', None)
        try:
            df.columns = map(str.upper,df.columns)
            for col,target_type in schema.items():
                if col not in df.columns:
                    continue

                if target_type == "Int64":
                    df[col] = df[col].astype("Int64")

                if target_type == "string":
                    df[col] = df[col].astype("string")

                if target_type=="float64":
                    df[col] = df[col].astype("float64")

                if target_type == "datetime64[ns,UTC]":
                    df[col] = pd.to_datetime(df[col],errors='coerce',utc=True, unit="s")

                if df[col].dtypes not in ["Int64","float64","string","datetime[ns,UTC]"]:
                    df[col] = df[col].astype("string")

            df.columns = map(str.lower,df.columns)
            return df

        except Exception as e:
            print(f"Error in type conversion:{str(e)}")




