# Airflow-ETL
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)  ![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)  ![Postgres](https://img.shields.io/badge/postgres-%23316192.svg?style=for-the-badge&logo=postgresql&logoColor=white)  ![Azure](https://img.shields.io/badge/azure-%230072C6.svg?style=for-the-badge&logo=microsoftazure&logoColor=white)  ![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
  ![Shell Script](https://img.shields.io/badge/shell_script-%23121011.svg?style=for-the-badge&logo=gnu-bash&logoColor=white) 
![Airflow Home](img/airflow-home.png)

Running ETL workflow in Apache Airflow

![Dag Run](img/dag-run.png)
## Docker commands

### Initialize airflow instance
```bash
docker-compose up airflow-init
```
```bash
docker-compose up 
```
### check if containers are up and running
```bash
docker ps
```
### shut down services
```bash
docker-compose down
```
### execute bash commands in container
```bash
docker exec -it <container name> bash
```
### extending  docker image
<!-- ```bash
docker-compose up -d --no-deps --build airflow-webserver airflow-scheduler
``` -->
```bash
docker build . --tag extending_airflow:latest
```
Change image name from apache-airflow IN `docker-compose.yaml` file.
```bash
image: ${AIRFLOW_IMAGE_NAME:-extending_airflow:latest}
```
```bash
docker-compose up -d --no-deps --build airflow-webserver airflow-scheduler
```