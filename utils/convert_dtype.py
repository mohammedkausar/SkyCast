import pandas as pd
from pandas import DataFrame


class DtypeConversion:
    '''Handles conversion of DataFrame columns to target data types.'''

    def type_convert(self, df: DataFrame, schema: dict):
        '''Convert DataFrame columns to defined types based on schema.'''
        pd.set_option('display.width', None)
        try:
            '''Convert all column names to uppercase for uniformity'''
            df.columns = map(str.upper, df.columns)

            '''Iterate through schema and cast each column to target type'''
            for col, target_type in schema.items():
                if col not in df.columns:
                    continue

                if target_type == 'Int64':
                    df[col] = df[col].astype('Int64')

                if target_type == 'string':
                    df[col] = df[col].astype('string')

                if target_type == 'float64':
                    df[col] = df[col].astype('float64')

                if target_type == 'datetime64[ns,UTC]':
                    df[col] = pd.to_datetime(df[col], errors='coerce', utc=True, unit='s')

                '''Fallback to string if type not matching expected formats'''
                if df[col].dtypes not in ['Int64', 'float64', 'string', 'datetime[ns,UTC]']:
                    df[col] = df[col].astype('string')

            '''Convert all column names back to lowercase'''
            df.columns = map(str.lower, df.columns)
            return df

        except Exception as e:
            '''Handle and log any type conversion errors'''
            print(f'Error in type conversion: {str(e)}')
