SELECT
    *,
    CASE 
        WHEN price_e10 > 250 OR price_e10 < 100 THEN TRUE 
        ELSE FALSE 
    END AS is_suspicious_price
FROM {{ ref('stg_fuel_prices') }}