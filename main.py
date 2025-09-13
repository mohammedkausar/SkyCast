import json
import os.path

import pandas as pd

from src.ETL.extract import ExtractCities

from src.ETL.transform import  TransformCities

# ec = ExtractCities()
# df = ec.city_from_parquet()
#
# df1 = ec.extract_data(df)
# df2 = pd.DataFrame(df1)
#
base = os.path.dirname(os.path.abspath(__file__))
# file = os.path.join(base,"staging","raw","raw_weather_data.parquet")
#
# df = pd.read_parquet(file)
# print(df.count())
file = json.load(open(os.path.join(base,"config","config.json")))
tc= TransformCities(file)

df=tc.transform_data()
print(df)