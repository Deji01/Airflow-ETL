FROM apache/airflow:2.3.4
COPY requirements.txt /requirements.txt
COPY data_engineering/ /dags/data_engineering/
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt