import numpy as np
import pandas as pd


class CleanData:
    """Performs sanitization and cleanup of DataFrames."""

    @staticmethod
    def sanitize_data_frame(df: pd.DataFrame):
        """Clean and standardize missing or null values in the DataFrame."""
        try:
            '''Define null-equivalent strings to replace'''
            null_equivalents = ['NA', 'N/A', 'NULL', 'null']

            '''Replace text-based nulls with np.nan'''
            df = df.replace(null_equivalents, np.nan)

            '''Drop columns that are entirely null'''
            df = df.dropna(axis=1, how='all')

            '''Replace np.nan with None for consistency'''
            df = df.replace(np.nan, None)
            df = df.where(pd.notna(df), None)

            '''Return sanitized DataFrame'''
            return df

        except Exception as e:
            '''Handle and log any sanitization errors'''
            print(f'Error sanitizing the data: {str(e)}')
            return df
