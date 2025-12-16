
    
    

with all_values as (

    select
        sentido_shape as value_field,
        count(*) as n_records

    from `rj-smtr`.`projeto_subsidio_sppo`.`aux_viagem_inicio_fim`
    group by sentido_shape

)

select *
from all_values
where value_field not in (
    'I','V','C'
)


