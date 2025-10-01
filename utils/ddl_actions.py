import json
import os.path
import psycopg2
from db_connect import get_config


class DdlActions:
    def __init__(self,config):
        self.schema = config["COLUMNS"]["TO_SQL_COLUMNS"]
        self.raw_table = config["TABLES"]["WEATHER_RAW"]

    def map_schema(self):
        try:
            col_str = ""
            for col,value in self.schema.items():
                col_str += f"{col} {value['type']} "

                if not value.get('nullable', True):
                    col_str += " NOT NULL"

                if value.get('primary_key', False):
                    col_str += " PRIMARY KEY"

                col_str += ", "
            return col_str.strip(", ")
        except Exception as e:
            print(f"Schema loading exception: {str(e)}")




    def create_denormalized_table(self):
        try:
            cols = self.map_schema()
            create_table_query = ""
            cfg = get_config()
            if cols:
                create_table_query += f"CREATE TABLE IF NOT EXISTS {self.raw_table} ({cols})"
                with psycopg2.connect(**cfg) as conn:
                    with conn.cursor() as cur:
                        cur.execute(create_table_query)
        except Exception as e:
            print(f"Cannot create table: {str(e)}")



base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
file = json.load(open(os.path.join(base, "config", "config.json")))
cd = DdlActions(file)
cd.create_denormalized_table()