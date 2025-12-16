
    
    

select
    id_viagem as unique_field,
    count(*) as n_records

from `rj-smtr`.`projeto_subsidio_sppo`.`registros_status_viagem`
where id_viagem is not null
group by id_viagem
having count(*) > 1


