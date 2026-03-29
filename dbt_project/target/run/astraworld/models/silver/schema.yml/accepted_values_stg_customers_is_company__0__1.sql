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
        is_company as value_field,
        count(*) as n_records

    from `astraworld_dw`.`customers`
    group by is_company

)

select *
from all_values
where value_field not in (
    '0','1'
)



      
    ) dbt_internal_test