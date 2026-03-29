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
      
    
    

select
    service_ticket as unique_field,
    count(*) as n_records

from `astraworld_dw`.`after_sales_raw`
where service_ticket is not null
group by service_ticket
having count(*) > 1



      
    ) dbt_internal_test