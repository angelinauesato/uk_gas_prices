{{ 
    config(
        materialized='view'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw_fuel_data', 'retailers_fuel_prices') }}
),

renamed_and_cast AS (
    SELECT
        -- Identifiers
        site_id,
        brand AS retailer_brand,
        
        -- Descriptive info
        station_name,
        address,
        postcode,
        
        -- Spatial data (Casting strings to float)
        latitude::float AS latitude,
        longitude::float AS longitude,
        
        -- Prices (Converting '0' strings to NULL and casting to numeric)
        NULLIF(price_e10, '0')::numeric AS price_e10,
        NULLIF(price_b7, '0')::numeric AS price_b7,
        NULLIF(price_e5, '0')::numeric AS price_e5,
        NULLIF(price_sdv, '0')::numeric AS price_sdv,
        
        -- Timestamps
        extracted_date::date AS extraction_date,
        processed_at AS loaded_at_utc
    FROM source
)

SELECT
    *
FROM renamed_and_cast
