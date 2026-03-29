
      insert into `astraworld_dw`.`after_sales` (`service_ticket`, `vin`, `customer_id`, `model`, `service_date`, `service_type`, `is_orphan_vin`, `created_at`, `_loaded_at`)
    (
       select `service_ticket`, `vin`, `customer_id`, `model`, `service_date`, `service_type`, `is_orphan_vin`, `created_at`, `_loaded_at`
       from `astraworld_dw`.`after_sales__dbt_tmp`
    )
  