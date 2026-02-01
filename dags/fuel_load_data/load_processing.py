import os
from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from airflow.utils.email import send_email
from datetime import timedelta

from datetime import datetime

RETAILERS = ["Applegreen", "Asda", "BP", "Esso"]
# Define the dbt path once at the top
DBT_PROJECT_DIR = "/opt/airflow/dbt/fuel_project"

@dag(
    start_date=datetime(2026, 1, 29),
    schedule=None,
    catchup=False,
    tags=['spark', 'fuel_prices']
)
def fuel_prices_unified_pipeline():
    
    s3_conn = BaseHook.get_connection('aws_s3_gas_prices')
    pg_conn = BaseHook.get_connection('postgres_raw')
    pg_jdbc_url = f"jdbc:postgresql://{pg_conn.host}:{pg_conn.port}/{pg_conn.schema}"

    # Success Notification
    @task
    def notify_completion(retailers):
        target_email = os.getenv('ALERT_EMAIL_ADDRESS')
    
        subject = f"✅ Fuel Pipeline Success: {datetime.now().strftime('%Y-%m-%d')}"
        
        body = f"""
        <h2>Fuel Pipeline Completed Successfully</h2>
        <p><b>Retailers Processed:</b> {', '.join(retailers)}</p>
        <p><b>Environment:</b> Production</p>
        <hr>
        <p>Sent to: {target_email}</p>
        """
        if target_email:
            send_email(
                to=target_email,
                subject=subject,
                html_content=body
            )
        else:
            print("Alert email not sent: ALERT_EMAIL_ADDRESS not found in environment.")
        
        return True

    spark_tasks = []
    
    for brand in RETAILERS:
        # The Spark Task: Writes to stg_raw_{brand}
        st = SparkSubmitOperator(
            task_id=f'spark_process_{brand}',
            conn_id='spark_main',
            application='/opt/airflow/dags/scripts/process_fuel.py',
            application_args=[
                brand, 
                "{{ (dag_run.conf.get('target_date', ds) | replace('-', '_')) }}"
            ],
            env_vars={
                "PG_URL": pg_jdbc_url,
                "PG_USER": pg_conn.login,
                "PG_PASSWORD": pg_conn.password,
                "AWS_ACCESS_KEY_ID": s3_conn.login,
                "AWS_SECRET_ACCESS_KEY": s3_conn.password
            },
            conf={
                "spark.ui.enabled": "false",
                "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.jars": "/opt/airflow/hadoop-aws-3.3.4.jar,/opt/airflow/aws-java-sdk-bundle-1.12.262.jar,/opt/airflow/postgresql-42.7.1.jar"
            }
        )
        spark_tasks.append(st)
    
    # The dbt Run Task
    dbt_run = BashOperator(
        task_id='dbt_run_transformation',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --profiles-dir .',
        # This ensures dbt can access the database passwords from your environment
        env={**os.environ},
        append_env=True,
        retries=3,                              # Try up to 3 times before giving up
        retry_delay=timedelta(minutes=5),       # Wait 5 minutes between tries
        retry_exponential_backoff=True,         # Wait longer each time (5, 10, 20 mins)
    )

    # The dbt Test Task
    dbt_test = BashOperator(
        task_id='dbt_test_integrity',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test --profiles-dir .',
        env={**os.environ},
        append_env=True,
        retries=1, # 1 time is enough for tests
        retry_delay=timedelta(minutes=2)
    )

    dbt_docs = BashOperator(
        task_id='dbt_docs_update',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt docs generate --profiles-dir .',
        env={**os.environ},
        append_env=True
    )

    # Dependency: All tasks must finish before Notification
    spark_tasks >> dbt_run >> dbt_test >> dbt_docs >> notify_completion(RETAILERS)

fuel_prices_unified_pipeline()
