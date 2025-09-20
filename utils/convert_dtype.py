import pandas as pd
from pandas import DataFrame


class DtypeConversion:
    def type_convert(self,df:DataFrame):
        pd.set_option('display.width', None)
        try:
            df=df.convert_dtypes()
            # for col in df.columns:
            #     dt = pd.to_datetime(df[col],errors='coerce',unit="s",utc=True)
            #     if dt.notna().mean() > 0.8:
            #         # print(dt)
            #         df[col] = dt
            #         continue
            #
            #     dt2 = pd.to_datetime(df[col], errors='coerce',format='%Y-%m-%d %H:%M:%S')
            #     if dt2.notna().mean() > 0.8:
            #         df[col] = dt2
            #         continue
                # print(dt)
                # print("+++++++++++++++++++++++++++")
                # print(dt2)
                # dt_num_float = pd.to_numeric(df[col], errors='coerce')
                # if dt_num_float.notna().mean() > 0.8:
                #     if(dt_num_float.dropna() % 1 == 0).all():
                #         df[col] = dt_num_float.astype("Int64")
                #     else:
                #         df[col] = dt_num_float.astype("float64")
                #     continue
                #
                #
                #
                # df[col] = df[col].astype("string")
            print(df.dtypes)
            return df

        except Exception as e:
            print(f"Error in type conversion:{str(e)}")




