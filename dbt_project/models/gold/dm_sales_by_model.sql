{{ config(
    materialized='incremental',
    unique_key=['periode', 'class', 'model'],
    incremental_strategy='merge',
    alias='dm_sales_by_model',
    on_schema_change='append_new_columns'
) }}

WITH filtered_sales AS (
    SELECT *
    FROM {{ ref('stg_sales') }}
    WHERE price IS NOT NULL
      AND invoice_date IS NOT NULL

    {% if is_incremental() %}
    AND _loaded_at >= (
        SELECT COALESCE(MAX(_loaded_at), '1970-01-01') FROM {{ this }}
    )
    {% endif %}
),

base AS (
    SELECT
        vin,
        model,
        invoice_date,
        price,

        CASE
            WHEN price < 250000000 THEN 'LOW'
            WHEN price <= 400000000 THEN 'MEDIUM'
            ELSE 'HIGH'
        END AS class
    FROM filtered_sales
)

SELECT
    DATE_FORMAT(invoice_date, '%Y-%m') AS periode,
    class,
    model,
    COUNT(*) AS unit_terjual,
    SUM(price) AS total,
    NOW() AS _loaded_at
FROM base
GROUP BY
    DATE_FORMAT(invoice_date, '%Y-%m'),
    class,
    model