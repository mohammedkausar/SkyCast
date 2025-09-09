import pandas as pd
import os



pd.set_option('display.width', None)
try:
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    file_path = os.path.join(BASE_DIR, "raw","current.city.list.json")
    city_df = pd.read_json(file_path)
    selected_city_df = city_df[["id","name","coord","country"]]
    indian_cities = selected_city_df.query("country == 'IN'").copy()
    indian_cities[["lon","lat"]] = indian_cities["coord"].apply(pd.Series)
    indian_cities.drop(columns=["coord"], inplace=True)
    indian_cities.to_parquet(os.path.join(BASE_DIR,"reference","dim_city.parquet"))
    pq = pd.read_parquet(os.path.join(BASE_DIR,"reference","dim_city.parquet"))
    print(pq.head())
except Exception as e:
    print(str(e))







