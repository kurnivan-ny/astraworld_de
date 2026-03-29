






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and count_service >= 1
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







