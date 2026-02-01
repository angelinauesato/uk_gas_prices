{{ 
    config(
        materialized='table'
    )
}}
WITH staged_data AS (
    SELECT 
        site_id,
        price_e10,
        price_b7,
        price_e5,
        price_sdv,
        extraction_date,
        loaded_at_utc
    FROM {{ ref('stg_fuel_prices') }}
),

-- Step 1: Get the latest unique price snapshot per station per day
deduplicated AS (
    SELECT DISTINCT ON (site_id, extraction_date)
        *
    FROM staged_data
    ORDER BY site_id, extraction_date, loaded_at_utc DESC
),

-- Step 2: Look at the PREVIOUS record's prices to see if anything changed
price_changes AS (
    SELECT 
        *,
        LAG(price_e10) OVER (PARTITION BY site_id ORDER BY extraction_date) as prev_e10,
        LAG(price_b7) OVER (PARTITION BY site_id ORDER BY extraction_date) as prev_b7,
        LAG(price_e5) OVER (PARTITION BY site_id ORDER BY extraction_date) as prev_e5,
        LAG(price_sdv) OVER (PARTITION BY site_id ORDER BY extraction_date) as prev_sdv
    FROM deduplicated
),

-- Step 3: Flag the rows where at least one price is different from the previous day
change_indicators AS (
    SELECT 
        *,
        CASE 
            WHEN price_e10 != prev_e10 
              OR price_b7 != prev_b7 
              OR price_e5 != prev_e5 
              OR price_sdv != prev_sdv 
              OR prev_e10 IS NULL THEN 1 
            ELSE 0 
        END as is_new_price_period
    FROM price_changes
),

-- Step 4: Create a "Group ID" for each period of stable prices
price_groups AS (
    SELECT 
        *,
        SUM(is_new_price_period) OVER (PARTITION BY site_id ORDER BY extraction_date) as period_group_id
    FROM change_indicators
)

-- Step 5: Final aggregation to get Start and End dates for each price period
SELECT 
    site_id,
    price_e10,
    price_b7,
    price_e5,
    price_sdv,
    MIN(extraction_date) as valid_from,
    CASE 
        WHEN MAX(extraction_date) = CURRENT_DATE THEN '9999-12-31'::date 
        ELSE MAX(extraction_date) 
    END as valid_to
FROM price_groups
GROUP BY site_id, price_e10, price_b7, price_e5, price_sdv, period_group_id