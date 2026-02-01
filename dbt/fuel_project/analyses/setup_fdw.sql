{# 
    This script sets up the Foreign Data Wrapper to link the Raw and Prod databases.
    Run this via: dbt compile && cat target/compiled/fuel_project/analyses/setup_fdw.sql | psql -h localhost -U airflow fuel_prices_prod
#}

-- 1. Enable Extension
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- 2. Define the Remote Server
-- We use Jinja to make the host/dbname configurable if they change later
{% set raw_db = "uk_gas_prices_raw" %}
{% set raw_host = "postgres" %}

CREATE SERVER IF NOT EXISTS raw_data_server
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host '{{ raw_host }}', port '5432', dbname '{{ raw_db }}');

-- 3. Create User Mapping
-- env_var('DB_PASSWORD') pulls the password from Docker/Shell environment
CREATE USER MAPPING IF NOT EXISTS FOR {{ target.user }}
SERVER raw_data_server
OPTIONS (
    user '{{ target.user }}', 
    password '{{ env_var("POSTGRES_PASSWORD") }}'
);

-- 4. Import the Tables
CREATE SCHEMA IF NOT EXISTS raw_data_import;

IMPORT FOREIGN SCHEMA public 
LIMIT TO (retailers_fuel_prices)
FROM SERVER raw_data_server 
INTO raw_data_import;