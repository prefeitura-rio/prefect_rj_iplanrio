
    
    

with all_values as (

    select
        sentido as value_field,
        count(*) as n_records

    from `rj-smtr`.`projeto_subsidio_sppo`.`aux_registros_status_trajeto`
    group by sentido

)

select *
from all_values
where value_field not in (
    'I','V','C'
)


