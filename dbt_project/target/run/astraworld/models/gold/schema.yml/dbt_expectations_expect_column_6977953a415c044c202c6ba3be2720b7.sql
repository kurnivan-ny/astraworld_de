select
      count(*) as failures,
      case
        when count(*) <> 0 then 'true'
        else 'false'
      end as should_warn,
      case
        when count(*) <> 0 then 'true'
        else 'false'
      end as should_error
    from (
      






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








      
    ) dbt_internal_test