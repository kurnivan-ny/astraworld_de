
    
    

select
    id as unique_field,
    count(*) as n_records

from `astraworld_dw`.`customer_addresses_raw`
where id is not null
group by id
having count(*) > 1


