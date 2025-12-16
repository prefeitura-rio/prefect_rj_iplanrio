
    
    

with all_values as (

    select
        variacao_itinerario as value_field,
        count(*) as n_records

    from (select * from `rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada` where DATA BETWEEN DATE('2022-01-01T00:00:00') AND DATE('2022-01-01T01:00:00'))
    group by variacao_itinerario

)

select *
from all_values
where value_field not in (
    'DD','DU','SS','RT','RM','DA','SA'
)


