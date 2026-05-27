FROM mcr.microsoft.com/devcontainers/python:3.10-bookworm

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
    flask-session==0.5.0 \
    faker \
    pandas \
    sqlalchemy \
    psycopg2-binary \
    apache-airflow==2.8.1 \
    dbt-core==1.11.11 \
    dbt-postgres==1.10.0 \
    astronomer-cosmos==1.3.2

EXPOSE 8080