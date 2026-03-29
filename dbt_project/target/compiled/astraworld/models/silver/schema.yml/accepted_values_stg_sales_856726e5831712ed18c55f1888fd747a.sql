
    
    

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


