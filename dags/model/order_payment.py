# Import required libraries
import pandas as pd
import numpy as np
from sqlalchemy import inspect
from logger.logger import logger
from connection.postgres import connect_to_db


# Initialize variables
dataset_url = "https://media.githubusercontent.com/media/alfianhid/Building-a-Data-Warehousing-Pipeline-using-Python-Docker-Airflow-PostgreSQL-and-dbt/master/data/raw/order_payment_dataset.csv"
columns = ['order_id',
           'payment_sequential',
           'payment_type',
           'payment_installments',
           'payment_value']
datatypes = {'order_id': pd.StringDtype,
           'payment_sequential': pd.Int16Dtype,
           'payment_type': pd.StringDtype,
           'payment_installments': pd.Int16Dtype,
           'payment_value': pd.Float32Dtype}


# Define ETL methods
@logger
def extract_dataset(dataset_url):
    print(f"Reading dataset from {dataset_url}...")
    df = pd.read_csv(dataset_url, names=columns, dtype=datatypes)
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
    df['payment_type'] = df['payment_type'].replace('credit_card', 'credit card')
    df['payment_type'] = df['payment_type'].replace('debit_card', 'debit card')
    df['payment_type'] = df['payment_type'].str.title()
    return df

@logger
def save_clean_dataset(df):
    df.to_csv('data/clean/order_payment_dataset.csv')

@logger
def is_tables_exists():
    db_engine = connect_to_db()
    print("Checking if tables already exists...")
    check_table_availability("raw_order_payment", db_engine)
    check_table_availability("clean_order_payment", db_engine)
    db_engine.dispose()

@logger
def order_payment_etl():
    db_engine = connect_to_db()

    raw_df = extract_dataset(dataset_url)
    raw_table_name = "raw_order_payment"

    clean_df = clean_dataset(raw_df)
    clean_table_name = "clean_order_payment"
    save_clean_dataset(clean_df)

    load_to_db(raw_df, raw_table_name, db_engine)
    load_to_db(clean_df, clean_table_name, db_engine)
    is_tables_exists()