from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta 
import psycopg2
import sys

# DATABASE
db_host = Variable.get("DB_HOST")
db_name = Variable.get("DB_NAME")
db_password = Variable.get("DB_PASSWORD")
db_port = Variable.get("DB_PORT")
db_user = Variable.get("DB_USER")

host = db_host
dbname = db_name
user = db_user
password = db_password
sslmode = "require"

# Constructing connection string
conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(
    host, user, dbname, password, sslmode
)


def swap_style_code():
    try:
        connection = psycopg2.connect(conn_string)
        print("Connection established")

    except psycopg2.Error as e:
        print(f"Error connecting to Postgres DB : {e}")
        sys.exit(1)

    curr = connection.cursor()

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

def swap_model():
    try:
        connection = psycopg2.connect(conn_string)
        print("Connection established")

    except psycopg2.Error as e:
        print(f"Error connecting to Postgres DB : {e}")
        sys.exit(1)

    curr = connection.cursor()

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

def update_model():
    try:
        connection = psycopg2.connect(conn_string)
        print("Connection established")

    except psycopg2.Error as e:
        print(f"Error connecting to Postgres DB : {e}")
        sys.exit(1)

    curr = connection.cursor()

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

def single_brand():
    try:
        connection = psycopg2.connect(conn_string)
        print("Connection established")

    except psycopg2.Error as e:
        print(f"Error connecting to Postgres DB : {e}")
        sys.exit(1)

    curr = connection.cursor()

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

def complete_price():
    try:
        connection = psycopg2.connect(conn_string)
        print("Connection established")

    except psycopg2.Error as e:
        print(f"Error connecting to Postgres DB : {e}")
        sys.exit(1)

    curr = connection.cursor()

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
    dag_id='clean_db',
    start_date=datetime(2022, 9, 10),
    schedule_interval='0 12 * * *') as dag:

    swap_style = PythonOperator(
        task_id='swap_style',
        python_callable=swap_style_code
    )

    model_swap = PythonOperator(
        task_id='model_swap',
        python_callable=swap_model
    )

    model_update = PythonOperator(
        task_id='model_update',
        python_callable=update_model
    )

    only_brand = PythonOperator(
        task_id='only_brand',
        python_callable=single_brand
    )

    full_price = PythonOperator(
        task_id='full_price',
        python_callable=complete_price
    )

    swap_style >> model_swap >> model_update >> only_brand >> full_price
