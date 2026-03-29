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
      
    
    

with all_values as (

    select
        priority as value_field,
        count(*) as n_records

    from `astraworld_dw`.`dm_customer_service`
    group by priority

)

select *
from all_values
where value_field not in (
    'HIGH','MED','LOW'
)



      
    ) dbt_internal_test