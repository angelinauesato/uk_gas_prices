# 🚀 UK Fuel Price Archiver
This project implements a robust, end-to-end data pipeline to collect, process, transform, and serve historical and current fuel price data for various retailers across the UK. Leveraging a modern data stack (Airflow, Spark, dbt, MinIO, PostgreSQL), the pipeline ensures data quality, efficiency, and provides a reliable foundation for analytics and reporting.
![data_pipeline](https://github.com/user-attachments/assets/c746a83e-6b0d-4168-8401-a627c2419fb6)


## Architecture
The pipeline follows a Medallion Architecture pattern (Bronze, Silver, Gold layers) and uses a Star Schema for its final data model.
# 🛠 Tech Stack
* **Airflow:** Orchestrates the entire pipeline, managing dependencies, scheduling, and error handling.
* **Spark:** Processes raw data, reads from S3 (MinIO), and writes to PostgreSQL.
* **dbt (Data Build Tool):** Transforms, tests, and documents data within PostgreSQL, creating a clean Star Schema.
* **MinIO (S3-compatible storage):** Stores raw JSON data collected from external sources.
* **PostgreSQL:** Serves as the data warehouse for staged, dimensional, and factual data.
* **Docker Compose:** Manages the local development environment for all services.

# 📂 Project Structure
```
├── dags/
│   └── fuel_fetch_data/
│       └── api_request.py    # Main DAG file - Ingestion JSON file to Minio
│   └── fuel_load_data/
│       └── load_processing.py    # Ingest the JSON to raw database/Postgres
│       └── 
│       └── 
|── dbt/
|     └──fuel_project/
|        └── analyses/
|           └── setup_fdw.sql # Foreign Data Wrapper to link the Raw and Prod databases - Needs to be run before running dbt
|        └── macros/
|        └── models/
|        └── seeds/
|        └── snapshots/
|        └── tests/
|        ── dbt_project.yml
|        ── profiles.yml # ignored and not pushed to the repo
├── docker-compose.yaml       # Airflow & MinIO services
|── .env                      # ignored and not pushed to the repo
└── README.md
```
# 🚀 Getting Started
  1. Prerequisites:<br />
     - Docker and Docker Compose installed.
     - Port 8080 (Airflow), 8081( dbt lineage) and 9001 (MinIO Console) are available.
     - Python 3.8+
  2. Configuration Parameters: <br />
     Ensure your .env file contains the following variables: <br />
      ```
        AIRFLOW_UID=50000
        AIRFLOW_USER=airflow
        AIRFLOW_PASSWORD=airflow
        AIRFLOW_GID=0
        POSTGRES_USER={USER}
        POSTGRES_PASSWORD={PASSWORD}
        POSTGRES_DB=airflow
        # Airflow UI Credentials
        AIRFLOW_WWW_USER_USERNAME={AIRFLOW_USER}
        AIRFLOW_WWW_USER_PASSWORD={AIRFLOW_PASSWORD}
        MINIO_ROOT_USER={MINIO_ROOT_USER}
        MINIO_ROOT_PASSWORD={MINIO_ROOT_PASSWORD}
        # python3 -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"
        FERNET_KEY={RUN_PYTHON_COMMENTED_CODE_ABOVE}
    
        ```

      Also download the jar files:
     - Download Postgres JDBC
     ```
      wget https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar
     ```
     - Download Hadoop AWS
     ```
      wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
      ```
     - Download AWS Java SDK Bundle
     ```
      wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
     ```
  4. Launch the Environment
     ```
     sudo docker compose -p uk_gas_prices up -d --build
     ```
  5. Setup Airflow Connections <br/>
     - To allow Airflow to talk to MinIO, create a connection in the Airflow UI (Admin -> Connections):
       - Conn Id: aws_s3_gas_prices
       - Conn Type: Amazon S3
       - Extra:
         ```
         {"endpoint_url": "http://minio:9000", "aws_access_key_id": "{MINIO_ROOT_USER}", "aws_secret_access_key": "{MINIO_ROOT_PASSWORD}"}<br />
         ```
       **Note:** You will need to replace: {MINIO_ROOT_USER} and {MINIO_ROOT_PASSWORD} with your information, from **.env** file.
  6. Creating raw and prod database inside of Postgres:
  ```
     sudo docker exec -it uk_gas_prices-postgres-1 psql -U {POSTGRES_USER}
     =# CREATE DATABASE uk_gas_prices_raw;
     =# CREATE DATABASE fuel_prices_prod;
     =# \c fuel_prices_prod;
     =# {NOW IT'S TIME TO RUN **The Foreign Data Wrapper to link the Raw and Prod databases.**}
  ```
   7. Create the profile.yml for dbt. Save it in: **uk_gas_prices/dbt/fuel_project/profile.yml**
   ```
    my_profile:
      target: dev
      outputs:
        dev:
          type: postgres
          database: fuel_prices_prod
          host: postgres
          pass: "{{ env_var('POSTGRES_PASSWORD') }}"
          port: 5432
          schema: public
          threads: 4
          type: postgres
          user: "{{ env_var('POSTGRES_USER') }}"
   ```
      
# 🕒 Scheduling<br />
The DAG is configured to run daily at **9:00 AM UTC** using the cron expression 0 9 * * *.
## Running the Pipeline

1.  **Unpause the `fuel_prices_unified_pipeline` DAG** in the Airflow UI.
2.  **Trigger the DAG manually** from the Airflow UI.

The DAG will:
* Scrape fuel price data for multiple UK retailers in parallel using Spark.
* Load raw data into MinIO (S3).
* Transform and model the data using dbt, creating dimensional and factual tables in PostgreSQL.
* Run data quality tests using dbt.
* Update dbt documentation.
* Send an email notification upon successful completion or failure.
## 📊 Data Coverage
|  Retailer     |   Status  |      Note                             |
|---------------|-----------|---------------------------------------|
| Applegreen    | ✅ Active | Reliable JSON endpoint.               |
| Asda          | ✅ Active | Direct government-mandated feed.      |
| BP            | ✅ Active | Requires custom User-Agent headers.   |
| Esso          | ✅ Active | Hosted on specialized data subdomain. |
| Shell         | ❌ Blocked| Protected by Akamai Bot Manager (403).|
# 🧪 Testing the Pipeline <br />
You can manually trigger a test run inside the scheduler container:
```
docker exec -it <container_id> airflow dags test uk_fuel_price_archive_v1 2026-01-24
```

# 💡 dbt Documentation
##  Lineage: <br />
To view the data lineage and documentation generated by dbt:

1.  Ensure the `airflow-webserver` port `8081` is mapped in `docker-compose.yaml`.
2.  Run the dbt docs server:
    ```bash
    docker compose exec airflow-webserver dbt docs serve --project-dir /opt/airflow/dbt/fuel_project --profiles-dir /opt/airflow/dbt/fuel_project --port 8081
    ```
3.  Open your browser to `http://localhost:8081`.

<img width="1783" height="690" alt="Screenshot 2026-01-31 at 6 43 37 PM" src="https://github.com/user-attachments/assets/da41b230-744b-4e59-9bfe-d20264f6bf1b" />


# 📜 Roadmap:<br />
- [ x ] Ingest data from retailers and save it inside of MinIO bucket.
- [ x ] Add a transformation layer to convert JSON raw layer (database in Postgres).
- [ x ] Update pipeline to trigger the transformation right after the ingestion of the JSON files.
- [ x ] Save data. Postgres. We have **raw** database: uk_gas_prices_raw and **prod** database: fuel_prices_prod
- [ x ] Update the pipeline to run dbt model to update the data.
- [ x ] Build a Slack or e-mail notification task for failure alerts.
- [ ] Build a DAG to run backfill, data prior to the project going online.
- [ ] Expand data sources to include more retailers or real-time APIs.
- [ ] Data Archiving - moves old records (older than 30 days) into a long-term storage table
- [ ] Integration with a dedicated BI tool (e.g., Superset, Metabase).

# 🔍 Troubleshooting & Known Issues
1. Shell 403 Forbidden Error <br />
**Issue:** The Shell API returns a 403 Forbidden error despite correct headers and session handling. <br />
**Cause:** Shell utilizes Akamai Bot Manager which performs TLS Fingerprinting. It identifies the Python requests library at the network handshake level as a non-browser entity.<br />
**Workaround:** Currently, Shell data is skipped to maintain pipeline health. Future versions may implement another solution.<br />
2. MinIO Connectivity<br />
**Issue:** botocore.exceptions.EndpointConnectionError <br />
**Solution:** Ensure  to add the connection in Airflow.
3. Task: dbt_run failing in production<br />
**Issue:** Task is failing the run in Airflow, message says missing staging models.
**Solution:** Re-creating the Foreign Data Wrapper, save inside of dbt project: uk_gas_prices/dbt/fuel_project/analyses/setup_fdw.sql

# 🗄️ Data Verification
To verify the integrity of the data lake without using the web UI, use the following CLI commands:

**List all archived files:**
```
docker exec -it uk_gas_prices-airflow-scheduler-1 python3 -c "from airflow.providers.amazon.aws.hooks.s3 import S3Hook; print(S3Hook(aws_conn_id='aws_s3_gas_prices').list_keys(bucket_name='uk-gas-price'))"
```
