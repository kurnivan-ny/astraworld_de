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
        service_type as value_field,
        count(*) as n_records

    from `astraworld_dw`.`after_sales_raw`
    group by service_type

)

select *
from all_values
where value_field not in (
    'BP','PM','GR'
)



      
    ) dbt_internal_test