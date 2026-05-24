FROM mcr.microsoft.com/devcontainers/python:3.10-bullseye

# Install system dependencies yang dibutuhkan
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Set working directory di dalam container
WORKDIR /workspace

# Install library Python untuk Synthetic Data, Airflow, dan dbt
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

# Set Environment Variable untuk Airflow Home
ENV AIRFLOW_HOME=/workspace/airflow

# Expose port untuk Web UI Airflow
EXPOSE 8080
