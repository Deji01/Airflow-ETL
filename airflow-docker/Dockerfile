FROM apache/airflow:2.0.1
COPY requirements.txt /requirements.txt
COPY data_engineering/ dags/
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt