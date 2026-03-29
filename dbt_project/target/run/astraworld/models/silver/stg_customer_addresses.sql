
      insert into `astraworld_dw`.`customer_addresses` (`id`, `customer_id`, `address`, `city`, `province`, `created_at`, `_loaded_at`)
    (
       select `id`, `customer_id`, `address`, `city`, `province`, `created_at`, `_loaded_at`
       from `astraworld_dw`.`customer_addresses__dbt_tmp`
    )
  