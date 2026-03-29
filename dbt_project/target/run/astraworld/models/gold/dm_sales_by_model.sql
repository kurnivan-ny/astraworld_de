
      insert into `astraworld_dw`.`dm_sales_by_model` (`periode`, `class`, `model`, `unit_terjual`, `total`, `_loaded_at`)
    (
       select `periode`, `class`, `model`, `unit_terjual`, `total`, `_loaded_at`
       from `astraworld_dw`.`dm_sales_by_model__dbt_tmp`
    )
  