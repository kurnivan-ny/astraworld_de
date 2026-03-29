
      insert into `astraworld_dw`.`dm_customer_service` (`customer_id`, `periode`, `vin`, `customer_name`, `address`, `count_service`, `priority`, `_loaded_at`)
    (
       select `customer_id`, `periode`, `vin`, `customer_name`, `address`, `count_service`, `priority`, `_loaded_at`
       from `astraworld_dw`.`dm_customer_service__dbt_tmp`
    )
  