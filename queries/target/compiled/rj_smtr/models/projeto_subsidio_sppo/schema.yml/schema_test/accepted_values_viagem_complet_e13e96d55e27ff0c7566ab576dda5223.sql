
    
    

with all_values as (

    select
        tipo_viagem as value_field,
        count(*) as n_records

    from `rj-smtr`.`projeto_subsidio_sppo`.`viagem_completa`
    group by tipo_viagem

)

select *
from all_values
where value_field not in (
    'Completa linha correta','Completa linha incorreta'
)


