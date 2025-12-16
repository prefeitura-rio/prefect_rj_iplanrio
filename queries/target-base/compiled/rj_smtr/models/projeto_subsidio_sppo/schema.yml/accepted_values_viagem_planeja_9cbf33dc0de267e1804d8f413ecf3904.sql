
    
    

with all_values as (

    select
        variacao_itinerario as value_field,
        count(*) as n_records

    from `rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada`
    group by variacao_itinerario

)

select *
from all_values
where value_field not in (
    'DD','DU','SS','RT','RM','DA','SA'
)


