from airflow import DAG
from airflow.operators.bash_operator import BashOperator 
from airflow.operators.pythonr import PythonOperator
from datetime import datetime, timedelta 
from data_engineering.clean_db import clean_data

default_args = {
    'owner' : 'Deji',
    'retry' : 5,
    'retry_delay' : timedelta(minutes=2)
}

with DAG(
    default_args=default_args,
    dag_id='etl_scrapy',
    start_date=datetime(2022, 9, 5, 12),
    schedule_interval='@daily') as dag:

    scrapy = BashOperator(
        task_id='scrapy',
        bash_command='cd data_engineering/shoes && scrapy crawl solesupplier'
    )
 
    clean_db = PythonOperator(
        task_id='clean_db',
        python_callable=clean_data
    )

    scrapy >> clean_db