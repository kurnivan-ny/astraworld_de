
    
    

with all_values as (

    select
        service_type as value_field,
        count(*) as n_records

    from `astraworld_dw`.`after_sales`
    group by service_type

)

select *
from all_values
where value_field not in (
    'BP','PM','GR'
)


