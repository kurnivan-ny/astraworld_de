

WITH filtered_sales AS (
    SELECT *
    FROM `astraworld_dw`.`sales`
    WHERE price IS NOT NULL
      AND invoice_date IS NOT NULL

    
    AND _loaded_at >= (
        SELECT COALESCE(MAX(_loaded_at), '1970-01-01') FROM `astraworld_dw`.`dm_sales_by_model`
    )
    
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