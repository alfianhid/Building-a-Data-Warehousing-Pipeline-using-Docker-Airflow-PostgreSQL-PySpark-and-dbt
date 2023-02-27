# Import required libraries
import pandas as pd
import numpy as np
from sqlalchemy import inspect
from logger.logger import logger
from connection.postgres import connect_to_db


# Initialize variables
dataset_url = "https://media.githubusercontent.com/media/alfianhid/Building-a-Data-Warehousing-Pipeline-using-Python-Docker-Airflow-PostgreSQL-and-dbt/master/data/raw/geolocation_dataset.csv"
columns = ['geolocation_zip_code_prefix',
           'geolocation_lat',
           'geolocation_lng',
           'geolocation_city',
           'geolocation_state']


# Define ETL methods
@logger
def extract_dataset(dataset_url):
    print(f"Reading dataset from {dataset_url}...")
    df = pd.read_csv(dataset_url, names=columns)
    return df

@logger
def check_table_availability(table_name, engine):
    if table_name in inspect(engine).get_table_names():
        print(f"{table_name!r} already exists in database!")
    else:
        print(f"{table_name} does not exist in database!")

@logger
def load_to_db(df, table_name, engine):
    print(f"Loading dataset to table: {table_name}...")
    df.to_sql(table_name, engine, if_exists="replace")
    check_table_availability(table_name, engine)

@logger
def clean_dataset(df):
    print("Cleaning dataset...")
    df = df.replace(r'^\s+$', np.nan, regex=True)
    df = df.drop_duplicates()
    df['geolocation_city'] = df['geolocation_city'].str.title()
    df['geolocation_state'] = df['geolocation_state'].str.upper()
    return df

@logger
def save_clean_dataset(df):
    df.to_csv('data/clean/geolocation_dataset.csv')

@logger
def is_tables_exists():
    db_engine = connect_to_db()
    print("Checking if tables already exists...")
    check_table_availability("raw_geolocation", db_engine)
    check_table_availability("clean_geolocation", db_engine)
    db_engine.dispose()

@logger
def geolocation_etl():
    db_engine = connect_to_db()

    raw_df = extract_dataset(dataset_url)
    raw_table_name = "raw_geolocation"

    clean_df = clean_dataset(raw_df)
    clean_table_name = "clean_geolocation"
    save_clean_dataset(clean_df)

    load_to_db(raw_df, raw_table_name, db_engine)
    load_to_db(clean_df, clean_table_name, db_engine)
    is_tables_exists()