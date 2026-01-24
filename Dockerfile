# Use the Airflow 3 base image
FROM apache/airflow:3.0.0

# Switch to root only if you need system-level OS packages
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch to airflow user for Python installations
USER airflow

# Install the Amazon provider (required for S3Hook)
RUN pip install --no-cache-dir "apache-airflow-providers-amazon>=9.0.0"