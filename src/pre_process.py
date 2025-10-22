import boto3
import pandas as pd
import os

pd.set_option('display.width', None)

try:
    '''Define base paths'''
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    file_path = os.path.join(BASE_DIR, 'staging', 'raw', 'city.list.json')

    '''Read raw city list JSON'''
    city_df = pd.read_json(file_path)

    '''Select required columns'''
    selected_city_df = city_df[['id', 'name', 'coord', 'country']]

    '''Filter only Indian cities'''
    indian_cities = selected_city_df.query('country == "IN"').copy()

    '''Split nested coord column into lon and lat'''
    indian_cities[['lon', 'lat']] = indian_cities['coord'].apply(pd.Series)

    '''Drop old coord column'''
    indian_cities.drop(columns=['coord'], inplace=True)

    '''Save processed data as parquet'''
    indian_cities.to_parquet(os.path.join(BASE_DIR, 'reference', 'dim_city.parquet'))

    '''Validate parquet load'''
    pq = pd.read_parquet(os.path.join(BASE_DIR, 'reference', 'dim_city.parquet'))

    '''Upload parquet to S3'''
    s3 = boto3.client('s3')
    s3.upload_file(
        os.path.join(BASE_DIR, 'reference', 'dim_city.parquet'),
        'skycast-weather-report',
        'reference/dim_city.parquet'
    )

except Exception as e:
    '''Catch and print any errors'''
    print(str(e))
