
    
    

with all_values as (

    select
        is_orphan_vin as value_field,
        count(*) as n_records

    from `astraworld_dw`.`after_sales`
    group by is_orphan_vin

)

select *
from all_values
where value_field not in (
    '0','1'
)


