{{
    config(
        materialized='table'
    )
}}

SELECT
    extraction_date,
    retailer_brand,
    AVG(price_e10) AS avg_price_e10_pence,
    {{ cents_to_pounds('AVG(price_e10)') }} AS avg_price_e10_gbp,
    AVG(price_b7) AS avg_price_b7_pence,
    {{ cents_to_pounds('AVG(price_b7)') }} AS avg_price_b7_gbp,
    COUNT(DISTINCT site_id) AS station_count
FROM {{ ref('stg_fuel_prices') }}
GROUP BY 1, 2
ORDER BY 1 DESC, 3 ASC
