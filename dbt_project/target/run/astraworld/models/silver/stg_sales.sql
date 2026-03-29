
      insert into `astraworld_dw`.`sales` (`vin`, `customer_id`, `model`, `invoice_date`, `price`, `created_at`, `_loaded_at`)
    (
       select `vin`, `customer_id`, `model`, `invoice_date`, `price`, `created_at`, `_loaded_at`
       from `astraworld_dw`.`sales__dbt_tmp`
    )
  