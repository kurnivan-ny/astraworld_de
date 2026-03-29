{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    alias='customers',
    on_schema_change='append_new_columns'
) }}

WITH raw AS (
    SELECT
        *,
        STR_TO_DATE(created_at, '%Y-%m-%d %H:%i:%s.%f') AS created_at_parsed
    FROM {{ source('bronze', 'customers_raw') }}

    {% if is_incremental() %}
    WHERE STR_TO_DATE(created_at, '%Y-%m-%d %H:%i:%s.%f') >
          (SELECT COALESCE(MAX(created_at), '1970-01-01') FROM {{ this }})
    {% endif %}
),

parsed AS (
    SELECT
        id,
        TRIM(name) AS name,
        dob AS dob_raw,
        CASE
            WHEN TRIM(name) REGEXP '^(PT|CV|UD)\\b' THEN 1
            ELSE 0
        END AS is_company,
        CASE
            -- YYYY-MM-DD  e.g. 1980-11-15
            WHEN dob REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                THEN STR_TO_DATE(dob, '%Y-%m-%d')
            -- YYYY/MM/DD  e.g. 1980/11/15
            WHEN dob REGEXP '^[0-9]{4}/[0-9]{2}/[0-9]{2}$'
                THEN STR_TO_DATE(dob, '%Y/%m/%d')
            -- DD/MM/YYYY  e.g. 15/11/1980
            WHEN dob REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
                THEN STR_TO_DATE(dob, '%d/%m/%Y')
            -- Dash-separated with 4-digit year: disambiguate DD-MM vs MM-DD
            WHEN dob REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
                THEN CASE
                    -- Middle segment > 12 → must be day → MM-DD-YYYY
                    WHEN CAST(SUBSTRING(dob, 4, 2) AS UNSIGNED) > 12
                        THEN STR_TO_DATE(dob, '%m-%d-%Y')
                    -- First segment > 12 → must be day → DD-MM-YYYY
                    WHEN CAST(SUBSTRING(dob, 1, 2) AS UNSIGNED) > 12
                        THEN STR_TO_DATE(dob, '%d-%m-%Y')
                    -- Both ≤ 12 → ambiguous, default to DD-MM-YYYY (Indonesian convention)
                    ELSE STR_TO_DATE(dob, '%d-%m-%Y')
                END
            ELSE NULL
        END AS dob,
        created_at_parsed AS created_at
    FROM raw
)

SELECT
    id,
    name,
    dob,
    dob_raw,
    is_company,
    created_at,
    NOW() AS _loaded_at
FROM parsed