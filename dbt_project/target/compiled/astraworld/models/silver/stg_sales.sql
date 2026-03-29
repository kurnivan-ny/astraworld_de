

WITH raw AS (
    SELECT
        *,
        STR_TO_DATE(created_at, '%Y-%m-%d %H:%i:%s.%f') AS created_at_parsed,
        STR_TO_DATE(invoice_date, '%Y-%m-%d') AS invoice_date_parsed
    FROM `astraworld_dw`.`sales_raw`

    
    WHERE STR_TO_DATE(created_at, '%Y-%m-%d %H:%i:%s.%f') >
          (SELECT COALESCE(MAX(created_at), '1970-01-01') FROM `astraworld_dw`.`sales`)
    
),

deduped AS (
    SELECT
        TRIM(vin) AS vin,
        customer_id,
        UPPER(TRIM(model)) AS model,
        invoice_date_parsed AS invoice_date,

        CAST(
            REPLACE(REPLACE(TRIM(price), '.', ''), ',', '') 
            AS UNSIGNED
        ) AS price,

        created_at_parsed AS created_at,

        ROW_NUMBER() OVER (
            PARTITION BY vin
            ORDER BY created_at_parsed DESC
        ) AS rn
    FROM raw
    WHERE vin IS NOT NULL
)

SELECT
    vin,
    customer_id,
    model,
    invoice_date,
    price,
    created_at,
    NOW() AS _loaded_at
FROM deduped
WHERE rn = 1