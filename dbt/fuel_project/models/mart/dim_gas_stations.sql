{{ 
    config(
        materialized='table'
    )
}}

WITH staged_data AS (
    SELECT
        *
    FROM {{ ref('stg_fuel_prices') }}
),

latest_station_info AS (
    SELECT DISTINCT ON (site_id)
        site_id,
        retailer_brand,
        station_name,
        address,
        postcode,
        latitude,
        longitude,
        loaded_at_utc as last_updated_at
    FROM staged_data
    ORDER BY site_id, loaded_at_utc DESC
)

SELECT
    *
FROM latest_station_info
