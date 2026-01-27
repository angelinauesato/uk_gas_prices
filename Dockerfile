FROM apache/airflow:2.8.1-python3.11

USER root

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# 1. Install dbt
RUN pip install --no-cache-dir dbt-core dbt-postgres

# 2. Install your project requirements
# Create a requirements.txt file in your project root with your libraries
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --user -r /requirements.txt