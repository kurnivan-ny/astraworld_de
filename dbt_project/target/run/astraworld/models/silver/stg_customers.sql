
      insert into `astraworld_dw`.`customers` (`id`, `name`, `dob`, `dob_raw`, `is_company`, `created_at`, `_loaded_at`)
    (
       select `id`, `name`, `dob`, `dob_raw`, `is_company`, `created_at`, `_loaded_at`
       from `astraworld_dw`.`customers__dbt_tmp`
    )
  