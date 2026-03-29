
    
    

select
    vin as unique_field,
    count(*) as n_records

from `astraworld_dw`.`sales`
where vin is not null
group by vin
having count(*) > 1


