from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from azure.storage.blob import ContainerClient
from datetime import datetime, timedelta
import glob
import json
import os
import psycopg2
import requests
from shutil import make_archive
import sys

# AZURE BLOB
container = Variable.get("AZ_BLOB_CONTAINER")
conn_string = Variable.get("AZ_SA_CONN_STRING")

# DATABASE
db_host = Variable.get("DB_HOST")
db_name = Variable.get("DB_NAME")
db_password = Variable.get("DB_PASSWORD")
db_port = Variable.get("DB_PORT")
db_user = Variable.get("DB_USER")

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

    return connection, curr

def create_table(curr, query):
    "Create Table in Database if it does not exist"
    try:
        curr.execute(query)

    except BaseException as e:
        print(e)


def store_db(curr, query, value):
    "Insert data into Database and commit changes"
    try:
        curr.execute(query, value)

    except BaseException as e:
        print(e)

# GET DATA

def extract():
    try:
        url = "https://ac.cnstrc.com/search/nike%20dunk?c=ciojs-client-2.29.2&key=key_XT7bjdbvjgECO5d8&i=b1f2bb9e-2bd1-49a8-865d-75557d8f8e3c&s=4&page=1&num_results_per_page=60"

        headers = {
            "authority": "ac.cnstrc.com",
            "accept": "*/*",
            "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
            "cache-control": "no-cache",
            "origin": "https://www.goat.com",
            "pragma": "no-cache",
            "sec-ch-ua": '"Google Chrome";v="105", "Not)A;Brand";v="8", "Chromium";v="105"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "cross-site",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
        }

        response = requests.get(url, headers=headers)
        result = response.json()

        step = 1

        current_dir = '/opt/airflow/dags/'
        data_dir = os.path.join(current_dir, "goat")

        filename = f"goat-{datetime.now().strftime('%d-%m-%Y')}-file-{step}.json"

        with open(f"{data_dir}/{filename}", "w", encoding="utf-8") as f:
            json.dump(result, f)

        i = 1

        # while requests.get(url, headers=headers).text is not None:
        for i in range(2, 168):
            step += 1

            url = f"https://ac.cnstrc.com/search/nike%20dunk?c=ciojs-client-2.29.2&key=key_XT7bjdbvjgECO5d8&i=b1f2bb9e-2bd1-49a8-865d-75557d8f8e3c&s=4&page={i}&num_results_per_page=60"

            response = requests.get(url, headers=headers)
            result = response.json()

            filename = f"goat-{datetime.now().strftime('%d-%m-%Y')}-file-{step}.json"
            with open(f"{data_dir}/{filename}", "w", encoding="utf-8") as f:
                json.dump(result, f)

            print(f"Step {step} Done!!!")
    
    except BaseException as e:
        print(e)

# PROCESS DATA

def transform(file):
    try:
        with open(file, "r") as f:
            data = json.load(f)

        for product in data["response"]["results"]:
            matched_terms = " ".join(product["matched_terms"])
            id = product["data"].get("id")
            variation_id = product["data"].get("variation_id")
            sku = product["data"].get("sku")
            slug = product["data"].get("slug")
            color = product["data"].get("color")
            category = product["data"].get("category")
            release_date = product["data"].get("release_date")
            release_date_year = product["data"].get("release_date_year")
            value = product["value"]
            product_type = product["data"].get("product_type")
            product_condition = product["data"].get("product_condition")
            count_for_product_condition = product["data"].get("count_for_product_condition")
            retail_price_cents = product["data"].get("retail_price_cents")
            retail_price_cents_gbp = product["data"].get("retail_price_cents_gbp")
            retail_price_cents_twd = product["data"].get("retail_price_cents_twd")
            retail_price_cents_cad = product["data"].get("retail_price_cents_cad")
            retail_price_cents_hkd = product["data"].get("retail_price_cents_hkd")
            retail_price_cents_sgd = product["data"].get("retail_price_cents_sgd")
            retail_price_cents_krw = product["data"].get("retail_price_cents_krw")
            retail_price_cents_cny = product["data"].get("retail_price_cents_cny")
            retail_price_cents_aud = product["data"].get("retail_price_cents_aud")
            retail_price_cents_jpy = product["data"].get("retail_price_cents_jpy")
            retail_price_cents_eur = product["data"].get("retail_price_cents_eur")
            lowest_price_cents = product["data"].get("lowest_price_cents")
            lowest_price_cents_krw = product["data"].get("lowest_price_cents_krw")
            lowest_price_cents_aud = product["data"].get("lowest_price_cents_aud")
            lowest_price_cents_cad = product["data"].get("lowest_price_cents_cad")
            lowest_price_cents_cny = product["data"].get("lowest_price_cents_cny")
            lowest_price_cents_sgd = product["data"].get("lowest_price_cents_sgd")
            lowest_price_cents_gbp = product["data"].get("lowest_price_cents_gbp")
            lowest_price_cents_eur = product["data"].get("lowest_price_cents_eur")
            lowest_price_cents_hkd = product["data"].get("lowest_price_cents_hkd")
            lowest_price_cents_jpy = product["data"].get("lowest_price_cents_jpy")
            lowest_price_cents_twd = product["data"].get("lowest_price_cents_twd")
            instant_ship_lowest_price_cents = product["data"].get(
                "instant_ship_lowest_price_cents"
            )
            instant_ship_lowest_price_cents_eur = product["data"].get(
                "instant_ship_lowest_price_cents_eur"
            )
            instant_ship_lowest_price_cents_gbp = product["data"].get(
                "instant_ship_lowest_price_cents_gbp"
            )
            instant_ship_lowest_price_cents_twd = product["data"].get(
                "instant_ship_lowest_price_cents_twd"
            )
            instant_ship_lowest_price_cents_sgd = product["data"].get(
                "instant_ship_lowest_price_cents_sgd"
            )
            instant_ship_lowest_price_cents_hkd = product["data"].get(
                "instant_ship_lowest_price_cents_hkd"
            )
            instant_ship_lowest_price_cents_cny = product["data"].get(
                "instant_ship_lowest_price_cents_cny"
            )
            instant_ship_lowest_price_cents_jpy = product["data"].get(
                "instant_ship_lowest_price_cents_jpy"
            )
            instant_ship_lowest_price_cents_cad = product["data"].get(
                "instant_ship_lowest_price_cents_cad"
            )
            instant_ship_lowest_price_cents_krw = product["data"].get(
                "instant_ship_lowest_price_cents_krw"
            )
            instant_ship_lowest_price_cents_aud = product["data"].get(
                "instant_ship_lowest_price_cents_aud"
            )
            image_url = product["data"].get("image_url")
            used_image_url = product["data"].get("used_image_url")

            yield (
                matched_terms,
                id,
                variation_id,
                sku,
                slug,
                color,
                category,
                release_date,
                release_date_year,
                value,
                product_type,
                product_condition,
                count_for_product_condition,
                retail_price_cents,
                retail_price_cents_gbp,
                retail_price_cents_twd,
                retail_price_cents_cad,
                retail_price_cents_hkd,
                retail_price_cents_sgd,
                retail_price_cents_krw,
                retail_price_cents_cny,
                retail_price_cents_aud,
                retail_price_cents_jpy,
                retail_price_cents_eur,
                lowest_price_cents,
                lowest_price_cents_krw,
                lowest_price_cents_aud,
                lowest_price_cents_cad,
                lowest_price_cents_cny,
                lowest_price_cents_sgd,
                lowest_price_cents_gbp,
                lowest_price_cents_eur,
                lowest_price_cents_hkd,
                lowest_price_cents_jpy,
                lowest_price_cents_twd,
                instant_ship_lowest_price_cents,
                instant_ship_lowest_price_cents_eur,
                instant_ship_lowest_price_cents_gbp,
                instant_ship_lowest_price_cents_twd,
                instant_ship_lowest_price_cents_sgd,
                instant_ship_lowest_price_cents_hkd,
                instant_ship_lowest_price_cents_cny,
                instant_ship_lowest_price_cents_jpy,
                instant_ship_lowest_price_cents_cad,
                instant_ship_lowest_price_cents_krw,
                instant_ship_lowest_price_cents_aud,
                image_url,
                used_image_url
            )
    except BaseException as e:
        print(e)

# SQL QUERY

create_table_query = """
    CREATE TABLE IF NOT EXISTS goat (
    date TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    matched_terms VARCHAR(40),
    id VARCHAR(100),
    variation_id VARCHAR(100),
    sku VARCHAR(40),
    slug VARCHAR(255),
    color VARCHAR(40),
    category VARCHAR(40),
    release_date INTEGER,
    release_date_year INTEGER,
    value VARCHAR(100),
    product_type VARCHAR(40),
    product_condition VARCHAR(40),
    count_for_product_condition INTEGER,
    retail_price_cents NUMERIC(12, 3),
    retail_price_cents_gbp NUMERIC(12, 3),
    retail_price_cents_twd NUMERIC(12, 3),
    retail_price_cents_cad NUMERIC(12, 3),
    retail_price_cents_hkd NUMERIC(12, 3),
    retail_price_cents_sgd NUMERIC(12, 3),
    retail_price_cents_krw NUMERIC(12, 3),
    retail_price_cents_cny NUMERIC(12, 3),
    retail_price_cents_aud NUMERIC(12, 3),
    retail_price_cents_jpy NUMERIC(12, 3),
    retail_price_cents_eur NUMERIC(12, 3),
    lowest_price_cents NUMERIC(12, 3),
    lowest_price_cents_krw NUMERIC(12, 3),
    lowest_price_cents_aud NUMERIC(12, 3),
    lowest_price_cents_cad NUMERIC(12, 3),
    lowest_price_cents_cny NUMERIC(12, 3),
    lowest_price_cents_sgd NUMERIC(12, 3),
    lowest_price_cents_gbp NUMERIC(12, 3),
    lowest_price_cents_eur NUMERIC(12, 3),
    lowest_price_cents_hkd NUMERIC(12, 3),
    lowest_price_cents_jpy NUMERIC(12, 3),
    lowest_price_cents_twd NUMERIC(12, 3),
    instant_ship_lowest_price_cents NUMERIC(12, 3),
    instant_ship_lowest_price_cents_eur NUMERIC(12, 3),
    instant_ship_lowest_price_cents_gbp NUMERIC(12, 3),
    instant_ship_lowest_price_cents_twd NUMERIC(12, 3),
    instant_ship_lowest_price_cents_sgd NUMERIC(12, 3),
    instant_ship_lowest_price_cents_hkd NUMERIC(12, 3),
    instant_ship_lowest_price_cents_cny NUMERIC(12, 3),
    instant_ship_lowest_price_cents_jpy NUMERIC(12, 3),
    instant_ship_lowest_price_cents_cad NUMERIC(12, 3),
    instant_ship_lowest_price_cents_krw NUMERIC(12, 3),
    instant_ship_lowest_price_cents_aud NUMERIC(12, 3),
    image_url VARCHAR(255),
    used_image_url VARCHAR(255)
    )
    """

insert_data_query = """
    INSERT INTO goat (
        matched_terms,
        id,
        variation_id,
        sku,
        slug,
        color,
        category,
        release_date,
        release_date_year,
        value,
        product_type,
        product_condition,
        count_for_product_condition,
        retail_price_cents,
        retail_price_cents_gbp,
        retail_price_cents_twd,
        retail_price_cents_cad,
        retail_price_cents_hkd,
        retail_price_cents_sgd,
        retail_price_cents_krw,
        retail_price_cents_cny,
        retail_price_cents_aud,
        retail_price_cents_jpy,
        retail_price_cents_eur,
        lowest_price_cents,
        lowest_price_cents_krw,
        lowest_price_cents_aud,
        lowest_price_cents_cad,
        lowest_price_cents_cny,
        lowest_price_cents_sgd,
        lowest_price_cents_gbp,
        lowest_price_cents_eur,
        lowest_price_cents_hkd,
        lowest_price_cents_jpy,
        lowest_price_cents_twd,
        instant_ship_lowest_price_cents,
        instant_ship_lowest_price_cents_eur,
        instant_ship_lowest_price_cents_gbp,
        instant_ship_lowest_price_cents_twd,
        instant_ship_lowest_price_cents_sgd,
        instant_ship_lowest_price_cents_hkd,
        instant_ship_lowest_price_cents_cny,
        instant_ship_lowest_price_cents_jpy,
        instant_ship_lowest_price_cents_cad,
        instant_ship_lowest_price_cents_krw,
        instant_ship_lowest_price_cents_aud,
        image_url,
        used_image_url
        )
    VALUES (
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s
    )
    """

# STORE DATA


def load():
    try:
        # create database connection
        connection, curr = create_connection()

        # create table if it doesn't exist
        create_table(curr, create_table_query)

        # read in json files in data dir
        files = glob.glob("/opt/airflow/dags/goat/*.json")

        for file in files:
            # get data from file
            items = transform(file)

            for value in items:
                # store data in database
                store_db(curr, insert_data_query, value)
                connection.commit()
    except BaseException as e:
        print(e)

    # close cursor and connection
    curr.close()
    connection.close()

def zip_dir():
    make_archive(
        f"/opt/airflow/dags/goat-{datetime.now().strftime('%d-%m-%Y')}",
        "zip",
        "/opt/airflow/dags/goat/"
    )

def blob_upload():
    try:
        container_client = ContainerClient.from_connection_string(conn_string, container)

        for path in glob.glob("/opt/airflow/dags/archive/*"):
            print(path)
            file = path.split('/')[-1]
            blob_client = container_client.get_blob_client(file)
            print(blob_client)
            with open(path, "rb") as data:
                blob_client.upload_blob(data)
                print(f"{file} uploaded to blob storage")
    except BaseException as e:
        print(e)

default_args = {
    'owner' : 'Deji',
    'retry' : 5,
    'retry_delay' : timedelta(minutes=2)
}

with DAG(
    default_args=default_args,
    dag_id='goat_etl',
    start_date=datetime(2022, 9, 25),
    schedule_interval='0 3 * * *') as dag:

    extract_data = PythonOperator(
        task_id='extract',
        python_callable=extract
    )

    transform_load = PythonOperator(
        task_id='transform_load',
        python_callable=load
    )

    archive_json_files = PythonOperator(
        task_id='archive_json_files',
        python_callable=zip_dir
    )

    zip_to_archive = BashOperator(
        task_id='zip_to_archive',
        bash_command='mv /opt/airflow/dags/*.zip /opt/airflow/dags/archive/'
    )

    archive_to_azure_blob = PythonOperator(
        task_id='archive_to_azure_blob',
        python_callable=blob_upload
    )

    delete_archive_files = BashOperator(
        task_id='delete_archive_files',
        bash_command= 'rm /opt/airflow/dags/archive/*'
    )

    delete_goat_files = BashOperator(
        task_id='delete_goat_files',
        bash_command= 'rm /opt/airflow/dags/goat/*'
    )

    extract_data >> transform_load >> archive_json_files >> zip_to_archive >> archive_to_azure_blob >> [delete_archive_files, delete_goat_files]