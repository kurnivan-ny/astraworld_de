select
      count(*) as failures,
      case
        when count(*) <> 0 then 'true'
        else 'false'
      end as should_warn,
      case
        when count(*) <> 0 then 'true'
        else 'false'
      end as should_error
    from (
      
    
    



select is_orphan_vin
from `astraworld_dw`.`after_sales`
where is_orphan_vin is null



      
    ) dbt_internal_test