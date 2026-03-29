
    
    

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


