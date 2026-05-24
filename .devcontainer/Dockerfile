FROM mcr.microsoft.com/devcontainers/python:3.10-bullseye

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
    faker \
    pandas \
    sqlalchemy \
    psycopg2-binary \
    apache-airflow==2.8.1 \
    dbt-core==1.7.0 \
    dbt-postgres==1.7.0 \
    astronomer-cosmos==1.3.2

EXPOSE 8080
