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
    vin as unique_field,
    count(*) as n_records

from `astraworld_dw`.`sales`
where vin is not null
group by vin
having count(*) > 1



      
    ) dbt_internal_test