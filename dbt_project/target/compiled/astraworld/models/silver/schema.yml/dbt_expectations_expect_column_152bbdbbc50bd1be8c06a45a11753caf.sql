






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and price >= 1 and price <= 5000000000
)
 as expression


    from `astraworld_dw`.`sales`
    

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







