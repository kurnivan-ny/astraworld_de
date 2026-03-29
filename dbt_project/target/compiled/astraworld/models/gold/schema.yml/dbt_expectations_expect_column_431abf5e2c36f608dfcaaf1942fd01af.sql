






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and total >= 1
)
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







