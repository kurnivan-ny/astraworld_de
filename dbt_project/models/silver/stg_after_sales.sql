{{ config(
    materialized='incremental',
    unique_key='service_ticket',
    incremental_strategy='merge',
    alias='after_sales',
    on_schema_change='append_new_columns'
) }}

WITH raw AS (
    SELECT
        *,
        STR_TO_DATE(created_at, '%Y-%m-%d %H:%i:%s.%f') AS created_at_parsed,
        STR_TO_DATE(service_date, '%Y-%m-%d') AS service_date_parsed
    FROM {{ source('bronze', 'after_sales_raw') }}

    {% if is_incremental() %}
    WHERE STR_TO_DATE(created_at, '%Y-%m-%d %H:%i:%s.%f') >
          (SELECT COALESCE(MAX(created_at), '1970-01-01') FROM {{ this }})
    {% endif %}
),

known_vins AS (
    SELECT DISTINCT UPPER(vin) AS vin
    FROM {{ source('bronze', 'sales_raw') }}
)

SELECT
    a.service_ticket,
    UPPER(a.vin)                    AS vin,
    a.customer_id,
    UPPER(a.model)                  AS model,
    a.service_date_parsed           AS service_date,
    a.service_type,
    CASE WHEN kv.vin IS NULL THEN 1 ELSE 0 END AS is_orphan_vin,
    a.created_at_parsed             AS created_at,
    NOW()                           AS _loaded_at
FROM raw a
LEFT JOIN known_vins kv 
    ON UPPER(a.vin) = kv.vin