
    
    

with all_values as (

    select
        class as value_field,
        count(*) as n_records

    from `astraworld_dw`.`dm_sales_by_model`
    group by class

)

select *
from all_values
where value_field not in (
    'LOW','MEDIUM','HIGH'
)


