import json
import os.path

from numpy.f2py.crackfortran import crackline


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
            if cols:
                create_table_query += f"CREATE TABLE IF NOT EXISTS {self.raw_table} ({cols})"
        except Exception as e:
            print(f"Cannot create table: {str(e)}")


    def test_method(self):
        pass


