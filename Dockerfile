FROM apache/airflow:2.8.1-python3.11

USER root

# Install Java (Required for PySpark) and Git
FROM apache/airflow:2.8.1-python3.11

USER root

COPY hadoop-aws-3.3.4.jar /opt/airflow/
COPY aws-java-sdk-bundle-1.12.262.jar /opt/airflow/
COPY postgresql-42.7.1.jar /opt/airflow/

# This ensures the 'airflow' user can actually use the files adding the permissions
RUN chmod 644 /opt/airflow/*.jar && chown airflow:root /opt/airflow/*.jar

# Install Java (Spark), Git, AND Postgres/Build dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       git \
       openjdk-17-jdk \
       ant \
       gcc \
       python3-dev \
       libpq-dev \
       procps \
    && ln -s /usr/lib/jvm/java-17-openjdk-arm64 /usr/lib/jvm/java-17-openjdk-amd64 \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Correct ENV format for JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# 2. Install Airflow Providers with constraints
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-amazon \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.11.txt"

# 3. Install dbt and PySpark
# Now that libpq-dev and gcc are installed, dbt-postgres can 
# successfully compile psycopg2 from source.
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    dbt-core==1.8.0 \
    dbt-postgres==1.8.0

# This downloads were saved in the project folder - Airflow was not reaching minio and postgres
# Download the Postgres Driver
# curl -O https://jdbc.postgresql.org/download/postgresql-42.7.1.jar

# Download the AWS SDK Bundle
# curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Download the Hadoop AWS connector
# curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar