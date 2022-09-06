from airflow import DAG
from airflow.operators.bash_operator import BashOperator 
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta 
import os
import sys

# DATABASE 

db_host = os.environ["DB_HOST"]
db_name = os.environ["DB_NAME"]
db_password = os.environ["DB_PASSWORD"]
db_port = os.environ["DB_PORT"]
db_user = os.environ["DB_USER"]

def create_connection():
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
    return {
        "connection" : connection,
        "curr" : curr
     }

def swap_style_code(connection, curr):
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

def swap_model(connection, curr):
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

def update_model(connection, curr):
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

def single_brand(connection, curr):
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

def complete_price(connection, curr):
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
    start_date=datetime(2022, 9, 5),
    schedule_interval='@daily') as dag:

    list_dir = BashOperator(
        task_id='list_dir',
        bash_command='echo "$(ls .)"'
    )

    clean_db = PythonOperator(
        task_id='clean_db',
        python_callable=clean_data
    )

# if __name__ == "__main__":
#     # create database connection
#     connection, curr = create_connection()

#     # clean database
#     clean_data(connection, curr)