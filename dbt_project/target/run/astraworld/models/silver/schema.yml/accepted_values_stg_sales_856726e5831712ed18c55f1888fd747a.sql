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
        model as value_field,
        count(*) as n_records

    from `astraworld_dw`.`sales`
    group by model

)

select *
from all_values
where value_field not in (
    'RAIZA','RANGGO','INNAVO','VELOS','ALTROS','GRAN MAX'
)



      
    ) dbt_internal_test