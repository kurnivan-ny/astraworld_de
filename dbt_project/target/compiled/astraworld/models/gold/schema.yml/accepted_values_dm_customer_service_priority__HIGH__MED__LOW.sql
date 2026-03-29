
    
    

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


