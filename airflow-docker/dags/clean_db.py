from airflow import DAG
from airflow.operators.bash_operator import BashOperator 
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta 
import os
import sys

# DATABASE
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
db_password = os.getenv("DB_PASSWORD")
db_port = os.getenv("DB_PORT")
db_user = os.getenv("DB_USER")

def create_connection(ti):
    "Create Database Connection"

    host = db_host
    dbname = db_name
    user = db_user
    password = db_password
    sslmode = "require"

    # Constructing connection string
    conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(
        host, user, dbname, password, sslmode
    )

    try:
        connection = psycopg2.connect(conn_string)
        print("Connection established")

    except psycopg2.Error as e:
        print(f"Error connecting to Postgres DB : {e}")
        sys.exit(1)

    curr = connection.cursor()

    ti.xcom_push(key='connection', value=connection)
    ti.xcom_push(key='curr', value=curr)

def swap_style_code(ti):
    connection = ti.xcom_pull(task_ids='create_connection', key='connection')
    curr = ti.xcom_pull(task_ids='create_connection', key='curr')
    try:
        curr.execute(
            """
                UPDATE sole_supplier 
                SET style_code = model , model = style_code
                WHERE model <> %s AND model ~ %s AND style_code is NULL;
            """,
            ("Dunk", "^[a-zA-Z]{2,3}[0-9]{3,4}[-][0-9]{3,4}$"),
        )
    except BaseException as e:
        print(f"swap style code and model (1) : {e}")

    connection.commit()
    curr.close()
    connection.close()

def swap_model(ti):
    connection = ti.xcom_pull(task_ids='create_connection', key='connection')
    curr = ti.xcom_pull(task_ids='create_connection', key='curr')
    try:
        curr.execute(
            """
                UPDATE sole_supplier 
                SET style_code = model , model = style_code
                WHERE model <> %s AND model ~ %s AND style_code is NULL;
            """,
            ("Dunk", "^[0-9]{6}[-][0-9]{3,4}$"),
        )
    except BaseException as e:
        print(f"swap style code and model (2) : {e}")

    connection.commit()
    curr.close()
    connection.close()

def update_model(ti):
    connection = ti.xcom_pull(task_ids='create_connection', key='connection')
    curr = ti.xcom_pull(task_ids='create_connection', key='curr')
    try:
        curr.execute(
            """
                UPDATE sole_supplier 
                SET brand = %s , model = %s
                WHERE brand = %s;
            """,
            ("Nike", "Dunk", "Dunk"),
        )
    except BaseException as e:
        print(f"Update Nike - Dunk : {e}")
    
    connection.commit()
    curr.close()
    connection.close()

def single_brand(ti):
    connection = ti.xcom_pull(task_ids='create_connection', key='connection')
    curr = ti.xcom_pull(task_ids='create_connection', key='curr')
    try:
        curr.execute(
            """
                DELETE FROM sole_supplier
                WHERE brand <> %s;
            """,
            ("Nike"),
        )
    except BaseException as e:
        print(f"Delete other brands : {e}")

    connection.commit()
    curr.close()
    connection.close()

def complete_price(ti):
    connection = ti.xcom_pull(task_ids='create_connection', key='connection')
    curr = ti.xcom_pull(task_ids='create_connection', key='curr')
    try:
        curr.execute(
            """
                DELETE FROM sole_supplier
                WHERE price IS NULL;
            """
        )
    except BaseException as e:
        print(f"Delete products without price : {e}")

    connection.commit()
    curr.close()
    connection.close()

default_args = {
    'owner' : 'Deji',
    'retry' : 5,
    'retry_delay' : timedelta(minutes=2)
}

with DAG(
    default_args=default_args,
    dag_id='etl_scrapy',
    start_date=datetime(2022, 9, 6),
    schedule_interval='30 0 * * *') as dag:

    create_connection = PythonOperator(
        task_id='create_connection',
        python_callable=create_connection
    )

    swap_style = PythonOperator(
        task_id='swap_style',
        python_callable=swap_style_code
    )

    swap_model = PythonOperator(
        task_id='swap_model',
        python_callable=swap_model
    )

    update_model = PythonOperator(
        task_id='update_model',
        python_callable=update_model
    )

    single_brand = PythonOperator(
        task_id='single_brand',
        python_callable=single_brand
    )

    complete_price = PythonOperator(
        task_id='complete_price',
        python_callable=complete_price
    )

    create_connection >> [swap_style, swap_model, update_model, single_brand, complete_price]
