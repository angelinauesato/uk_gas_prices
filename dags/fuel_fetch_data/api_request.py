import json
import requests
import time
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.dagrun_operator import TriggerDagRunOperator

RETAILERS = [
    {"brand": "Applegreen", "url": "https://www.applegreenstores.com/fuel-prices/data.json"},
    {"brand": "Asda", "url": "https://storelocator.asda.com/fuel_prices_data.json"},
    {"brand": "BP", "url": "https://www.bp.com/en_gb/united-kingdom/home/fuelprices/fuel_prices_data.json"},
   # {"brand": "Shell", "url": "https://www.shell.co.uk/motorist/fuel-prices/_jcr_content/root/main/section/fuel_prices.ext.json"},
    {"brand": "Esso", "url": "https://fuelprices.esso.co.uk/latestdata.json"}
]

default_args = {
    'owner': 'airflow',
    'retries': 3,                            # Number of retries
    'retry_delay': timedelta(seconds=30),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=5), # But no more than 5 mins
}

@dag(
    dag_id='uk_fuel_price_archive',
    default_args=default_args,
    schedule='0 9 * * *',  # Runs at 0 minutes, 9 hours (9:00 AM) every day
    start_date=datetime(2024, 1, 1),
    catchup=False
)
def fuel_price_collector_dag():

    @task
    def fetch_all_retailers(target_date_nodash):
        s3_hook = S3Hook(aws_conn_id="aws_s3_gas_prices")
        bucket_name = "uk-gas-price"
        # Initialize the Session and Headers
        session = requests.Session()
        common_headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-GB,en;q=0.9',
        }
        session.headers.update(common_headers)
        results = []

        # Ensure bucket exists
        if not s3_hook.check_for_bucket(bucket_name):
            s3_hook.create_bucket(bucket_name)

        for retailer in RETAILERS:
            brand = retailer['brand']
            url = retailer['url']

            try:
                if brand == "Shell":
                    # Visit home page first to pick up Akamai cookies
                    session.get("https://www.shell.co.uk/motorist/fuel-prices.html", timeout=15)
                    # Use a slightly different endpoint that is often less protected
                    shell_url = "https://www.shell.co.uk/motorist/fuel-prices/_jcr_content/root/main/section/fuel_prices.ext.json"
                    response = session.get(shell_url, timeout=20)
                else:
                    # For BP, Esso, etc., use the session with the common headers
                    response = session.get(url, timeout=20)
                
                response.raise_for_status()
                data = response.json()

                s3_key = f"gas_prices_uk/{brand}/{target_date_nodash}.json"
                s3_hook.load_string(
                    string_data=json.dumps(data),
                    key=s3_key,
                    bucket_name=bucket_name,
                    replace=True
                )
                print(f"Uploaded {brand}. Sleeping for 2 seconds to avoid MinIO deadlock...")
                time.sleep(2)

                results.append(f"Successfully uploaded {brand}")
            except Exception as e:
                # This forces Airflow to see the error and trigger a retry
                raise Exception(f"Retailer {retailer} failed to upload: {str(e)}")
        
        return results

    formatted_date = "{{ data_interval_end | ds | replace('-', '_') }}"
    fetch_data = fetch_all_retailers(formatted_date)

    trigger_processing = TriggerDagRunOperator(
        task_id="trigger_unified_pipeline",
        trigger_dag_id="fuel_prices_unified_pipeline", # Must match the other DAG's ID
        wait_for_completion=False, # Just trigger and finish
        reset_dag_run=True,        # Allows it to trigger again on re-runs
        conf={"target_date": "{{ data_interval_end | ds }}"}
    )

    # Set the dependency chain
    fetch_data >> trigger_processing

# Initialize the DAG by calling the function
fuel_price_collector_dag()