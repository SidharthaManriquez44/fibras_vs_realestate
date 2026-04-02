FROM apache/airflow:2.9.0-python3.10

USER root

RUN apt-get update && apt-get install -y build-essential

COPY requirements.txt /requirements.txt

USER airflow

RUN pip install --no-cache-dir -r /requirements.txt

COPY src /opt/airflow/src
COPY dags /opt/airflow/dags

ENV PYTHONPATH="/opt/airflow/src"