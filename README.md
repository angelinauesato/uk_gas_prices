# 🚀 UK Fuel Price Archiver (v1)
A robust data engineering pipeline built with **Airflow ** and **Docker** to archive daily fuel prices from major UK retailers (Asda, BP, Esso, etc.)
into a **MinIO** (S3-compatible) Data Lake.
# 🛠 Tech Stack
* Orchestrator: Airflow (TaskFlow API, TriggerDagRunOperator, S3Hook, SparkSession, SparkSubmitOperator, PostgresHook, BaseHook, TriggerDagRunOperator, BashOperator)
* Storage: MinIO (S3 API)
* Containerization: Dockerfile & Docker Compose
* Language: Python 3.12 (Requests, Boto3)

# 📂 Project Structure
```
├── dags/
│   └── fuel_fetch_data/
│       └── api_request.py    # Main DAG file
|── dbt/
|     └──fuel_project/
|        └── analyses/
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
     - Port 8080 (Airflow) and 9001 (MinIO Console) are available.
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
  3. Launch the Environment
     ```
     docker compose -p uk_gas_prices up -d
     ```
  4. Setup Airflow Connections <br/>
     - To allow Airflow to talk to MinIO, create a connection in the Airflow UI (Admin -> Connections):
       - Conn Id: aws_s3_gas_prices
       - Conn Type: Amazon S3
       - Extra:
         ```
         {"endpoint_url": "http://minio:9000", "aws_access_key_id": "{MINIO_ROOT_USER}", "aws_secret_access_key": "{MINIO_ROOT_PASSWORD}"<br />
         ```
       **Note:** You will need to replace: {MINIO_ROOT_USER} and {MINIO_ROOT_PASSWORD} with your information, from **.env** file.
# 🕒 Scheduling<br />
The DAG is configured to run daily at **9:00 AM UTC** using the cron expression 0 9 * * *.
# 📊 Data Coverage
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
# 💡 Lineage: <br />
```
docker compose exec airflow-webserver dbt docs serve --project-dir /opt/airflow/dbt/fuel_project --profiles-dir /opt/airflow/dbt/fuel_project --port 8081
```
<img width="1783" height="690" alt="Screenshot 2026-01-31 at 6 43 37 PM" src="https://github.com/user-attachments/assets/da41b230-744b-4e59-9bfe-d20264f6bf1b" />


# 📜 Roadmap:<br />
- [ x ] Ingest data from retailers and save it inside of MinIO bucket.
- [ x ] Add a transformation layer to convert JSON raw layer (database in Postgres).
- [ x ] Update pipeline to trigger the transformation right after the ingestion of the JSON files.
- [ x ] Save data. Postgres. We have **raw** database: uk_gas_prices_raw and **prod** database: fuel_prices_prod
- [ x ] Update the pipeline to run dbt model to update the data.
- [ ] Data Archiving - moves old records (older than 30 days) into a long-term storage table
- [ x ] Build a Slack or e-mail notification task for failure alerts.

# 🔍 Troubleshooting & Known Issues
1. Shell 403 Forbidden Error <br />
**Issue:** The Shell API returns a 403 Forbidden error despite correct headers and session handling. <br />
**Cause:** Shell utilizes Akamai Bot Manager which performs TLS Fingerprinting. It identifies the Python requests library at the network handshake level as a non-browser entity.<br />
**Workaround:** Currently, Shell data is skipped to maintain pipeline health. Future versions may implement another solution.<br />
2. MinIO Connectivity<br />
**Issue:** botocore.exceptions.EndpointConnectionError <br />
**Solution:** Ensure  to add the connection in Airflow.

# 🗄️ Data Verification
To verify the integrity of the data lake without using the web UI, use the following CLI commands:

**List all archived files:**
```
docker exec -it uk_gas_prices-airflow-scheduler-1 python3 -c "from airflow.providers.amazon.aws.hooks.s3 import S3Hook; print(S3Hook(aws_conn_id='aws_s3_gas_prices').list_keys(bucket_name='uk-gas-price'))"
```
