FROM apache/airflow:2.3.4
COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt

FROM alpine
COPY --from=library/docker:latest /usr/local/bin/docker /usr/bin/docker
COPY --from=docker/compose:latest /usr/local/bin/docker-compose /usr/bin/docker-compose

WORKDIR /opt/airflow


COPY docker-compose.yaml /docker-compose.yaml
COPY dags/* /dags/
COPY logs/* /logs/
COPY plugins/* /plugins/

ENV AIRFLOW_UID=1000
ENV AIRFLOW_GID=0

# RUN docker run --rm -it -v $PWD:$PWD -w $PWD -v /var/run/docker.sock:/var/run/docker.sock
COPY airflow.sh /airflow.sh
RUN chmod + x /airflow.sh && /airflow.sh bash && docker-compose up airflow-init

EXPOSE 8080
CMD ["docker-compose", "up"]