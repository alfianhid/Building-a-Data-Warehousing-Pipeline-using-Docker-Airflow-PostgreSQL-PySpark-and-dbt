FROM apache/airflow:2.2.3

ENV AIRFLOW_HOME=/opt/airflow

USER root

COPY requirements.txt /usr/local/airflow/requirements.txt

RUN pip install --no-cache-dir -U pip setuptools wheel psutil
RUN pip install --no-cache-dir -r /usr/local/airflow/requirements.txt

# Ref: https://airflow.apache.org/docs/docker-stack/recipes.html
SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

WORKDIR $AIRFLOW_HOME

COPY scripts scripts
RUN chmod +x scripts

USER $AIRFLOW_UID