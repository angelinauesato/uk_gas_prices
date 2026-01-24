import json
import requests
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

RETAILERS = [
    {"brand": "Applegreen", "url": "https://www.applegreenstores.com/fuel-prices/data.json"},
    {"brand": "Asda", "url": "https://storelocator.asda.com/fuel_prices_data.json"},
    {"brand": "BP", "url": "https://www.bp.com/en_gb/united-kingdom/home/fuelprices/fuel_prices_data.json"},
   # {"brand": "Shell", "url": "https://www.shell.co.uk/motorist/fuel-prices/_jcr_content/root/main/section/fuel_prices.ext.json"},
    {"brand": "Esso", "url": "https://fuelprices.esso.co.uk/latestdata.json"}
]

@dag(
    dag_id='uk_fuel_price_archive_v1',
    schedule='0 9 * * *',  # Runs at 0 minutes, 9 hours (9:00 AM) every day
    start_date=datetime(2024, 1, 1),
    catchup=False
)
def fuel_price_collector_dag():

    @task
    def fetch_all_retailers():
        s3_hook = S3Hook(aws_conn_id="aws_s3_gas_prices")
        bucket_name = "uk-gas-price"
        # 1. Initialize the Session and Headers
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

                s3_key = f"gas_prices_uk/{brand}/{datetime.now().strftime('%Y_%m_%d')}.json"
                s3_hook.load_string(
                    string_data=json.dumps(data),
                    key=s3_key,
                    bucket_name=bucket_name,
                    replace=True
                )
                results.append(f"Successfully uploaded {brand}")
            except Exception as e:
                results.append(f"Failed {brand}: {str(e)}")
        
        return results

    fetch_all_retailers()

# Initialize the DAG
fuel_price_collector_dag()