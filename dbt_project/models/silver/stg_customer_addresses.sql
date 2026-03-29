{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='merge',
    alias='customer_addresses',
    on_schema_change='append_new_columns'
) }}

WITH raw AS (
    SELECT * FROM {{ source('bronze', 'customer_addresses_raw') }}
    {% if is_incremental() %}
    WHERE STR_TO_DATE(created_at, '%Y-%m-%d %H:%i:%s.%f') > (
        SELECT COALESCE(MAX(created_at), '1970-01-01') FROM {{ this }}
    )
    {% endif %}
),

normalized AS (
    SELECT
        id,
        customer_id,
        TRIM(address) AS address,
        CONCAT(
            UPPER(LEFT(TRIM(city), 1)),
            LOWER(SUBSTRING(TRIM(city), 2))
        ) AS city,
        CONCAT(
            UPPER(LEFT(TRIM(province), 1)),
            LOWER(SUBSTRING(TRIM(province), 2))
        ) AS province,
        STR_TO_DATE(created_at, '%Y-%m-%d %H:%i:%s.%f') AS created_at,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY STR_TO_DATE(created_at, '%Y-%m-%d %H:%i:%s.%f') DESC
        ) AS rn
    FROM raw
)

SELECT
    id,
    customer_id,
    address,
    city,
    province,
    created_at,
    NOW()  AS _loaded_at
FROM normalized
WHERE rn = 1
