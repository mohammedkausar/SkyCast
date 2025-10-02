import numpy as np
import pandas as pd


class CleanData:
    @staticmethod
    def sanitize_data_frame(df: pd.DataFrame):
        try:
            null_equivalents=['NA','N/A','NULL','null']
            df = df.replace(null_equivalents, np.nan)
            df = df.dropna(axis=1,how='all')
            df = df.replace(np.nan,None)
            df= df.where(pd.notna(df), None)
            return df
        except Exception as e:
            print(f"Error sanitizing the data: {str(e)}")
            return df

