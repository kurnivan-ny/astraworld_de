




    with grouped_expression as (
    select
        
        
    
  


    


regexp_instr(periode, '^[0-9]{4}-[0-9]{2}$', 1, 1)


 > 0
 as expression


    from `astraworld_dw`.`dm_sales_by_model`
    

),
validation_errors as (

    select
        *
    from
        grouped_expression
    where
        not(expression = true)

)

select *
from validation_errors




