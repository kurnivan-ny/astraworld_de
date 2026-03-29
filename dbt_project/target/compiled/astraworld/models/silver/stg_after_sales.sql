

WITH raw AS (
    SELECT
        *,
        STR_TO_DATE(created_at, '%Y-%m-%d %H:%i:%s.%f') AS created_at_parsed,
        STR_TO_DATE(service_date, '%Y-%m-%d') AS service_date_parsed
    FROM `astraworld_dw`.`after_sales_raw`

    
    WHERE STR_TO_DATE(created_at, '%Y-%m-%d %H:%i:%s.%f') >
          (SELECT COALESCE(MAX(created_at), '1970-01-01') FROM `astraworld_dw`.`after_sales`)
    
),

known_vins AS (
    SELECT DISTINCT UPPER(vin) AS vin
    FROM `astraworld_dw`.`sales_raw`
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