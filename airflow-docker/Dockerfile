FROM apache/airflow:2.3.4
COPY requirements.txt /requirements.txt
COPY docker-compose.yaml /docker-compose.yaml
COPY dags/* /dags/
COPY logs/* /logs/
COPY plugins/* /plugins/
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt
RUN docker-compose up airflow-init
EXPOSE 8080
CMD ["docker-compose", "up"]