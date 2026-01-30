-- 1. NOTE: The "CREATE DATABASE" command must be run while connected to 
-- the 'postgres' or 'airflow' database. 
-- create database 'uk_gas_prices_raw'.
/*
CREATE DATABASE uk_gas_prices_raw
    WITH
    OWNER = airflow
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.utf8'
    LC_CTYPE = 'en_US.utf8'
    LOCALE_PROVIDER = 'libc'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;
*/

-- 2. Create the Table (Run this while connected to uk_gas_prices_raw)
CREATE TABLE IF NOT EXISTS retailers_fuel_prices (
    brand VARCHAR(50),
    site_id VARCHAR(100),
    station_name VARCHAR(255),
    address VARCHAR(255),
    postcode VARCHAR(20),
    latitude VARCHAR(255),
    longitude VARCHAR(255),
    price_b7 VARCHAR(255),
    price_e10 VARCHAR(255),
    price_e5 VARCHAR(255),
    price_sdv VARCHAR(255),
    extracted_date VARCHAR(255),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Add Indexes
CREATE INDEX IF NOT EXISTS idx_brand ON retailers_fuel_prices(brand);
CREATE INDEX IF NOT EXISTS idx_date ON retailers_fuel_prices(extracted_date);