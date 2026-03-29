

WITH filtered_service AS (
    SELECT *
    FROM `astraworld_dw`.`after_sales`
    WHERE service_date IS NOT NULL

    
    AND _loaded_at >= (
        SELECT COALESCE(MAX(_loaded_at), '1970-01-01') FROM `astraworld_dw`.`dm_customer_service`
    )
    
),

service_counts AS (
    SELECT
        customer_id,
        YEAR(service_date) AS periode,
        COUNT(*) AS count_service
    FROM filtered_service
    GROUP BY customer_id, YEAR(service_date)
),

latest_vin AS (
    SELECT customer_id, vin
    FROM (
        SELECT
            customer_id,
            vin,
            ROW_NUMBER() OVER (
                PARTITION BY customer_id
                ORDER BY invoice_date DESC
            ) AS rn
        FROM `astraworld_dw`.`sales`
    ) t
    WHERE rn = 1
),

latest_addr AS (
    SELECT customer_id, address, city, province
    FROM (
        SELECT
            customer_id,
            address,
            city,
            province,
            ROW_NUMBER() OVER (
                PARTITION BY customer_id
                ORDER BY created_at DESC
            ) AS rn
        FROM `astraworld_dw`.`customer_addresses`
    ) t
    WHERE rn = 1
)

SELECT
    sc.customer_id,
    sc.periode,
    lv.vin,
    c.name AS customer_name,

    COALESCE(
        CONCAT(la.address, ', ', la.city, ', ', la.province),
        'Alamat tidak tersedia'
    ) AS address,

    sc.count_service,

    CASE
        WHEN sc.count_service > 10 THEN 'HIGH'
        WHEN sc.count_service >= 5 THEN 'MED'
        ELSE 'LOW'
    END AS priority,

    NOW() AS _loaded_at

FROM service_counts sc
LEFT JOIN latest_vin lv 
    ON sc.customer_id = lv.customer_id
LEFT JOIN `astraworld_dw`.`customers` c  
    ON sc.customer_id = c.id
LEFT JOIN latest_addr la 
    ON sc.customer_id = la.customer_id