
    
    

select
    service_ticket as unique_field,
    count(*) as n_records

from `astraworld_dw`.`after_sales`
where service_ticket is not null
group by service_ticket
having count(*) > 1


