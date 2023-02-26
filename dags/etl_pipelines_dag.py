from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from model.customers import customers_etl
from model.geolocation import geolocation_etl
from model.order_items import order_items_etl
from model.order_payment import order_payment_etl
from model.order_reviews import order_reviews_etl
from model.orders import orders_etl
from model.product_translation import product_translation_etl
from model.products import products_etl
from model.sellers import sellers_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    'etl_pipelines_dag',
    default_args=default_args,
    description='ETL Pipelines DAG',
    schedule_interval="@once",
    start_date=datetime.strptime("2022-02-27", '%Y-%m-%d'),
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id='customers',
        python_callable=customers_etl,
    )

    task2 = PythonOperator(
        task_id='geolocation',
        python_callable=geolocation_etl,
    )

    task3 = PythonOperator(
        task_id='order_items',
        python_callable=order_items_etl,
    )

    task4 = PythonOperator(
        task_id='order_payment',
        python_callable=order_payment_etl,
    )

    task5 = PythonOperator(
        task_id='order_reviews',
        python_callable=order_reviews_etl,
    )

    task6 = PythonOperator(
        task_id='orders',
        python_callable=orders_etl,
    )

    task7 = PythonOperator(
        task_id='product_translation',
        python_callable=product_translation_etl,
    )

    task8 = PythonOperator(
        task_id='products',
        python_callable=products_etl,
    )

    task9 = PythonOperator(
        task_id='sellers',
        python_callable=sellers_etl,
    )

    task2.set_upstream(task1)
    task3.set_upstream(task2)
    task4.set_upstream(task3)
    task5.set_upstream(task4)
    task6.set_upstream(task5)
    task7.set_upstream(task6)
    task8.set_upstream(task7)
    task9.set_upstream(task8)