






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and periode >= 2000 and periode <= 2100
)
 as expression


    from `astraworld_dw`.`dm_customer_service`
    

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







