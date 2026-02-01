{{
    config(
        materialized='table'
    )
}}

SELECT 
    *
FROM {{ ref('fct_fuel_prices') }}
WHERE valid_to IS NULL
