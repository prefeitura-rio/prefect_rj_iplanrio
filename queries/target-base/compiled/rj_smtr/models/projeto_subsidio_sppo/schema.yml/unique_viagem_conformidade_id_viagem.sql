
    
    

with dbt_test__target as (

  select id_viagem as unique_field
  from `rj-smtr`.`projeto_subsidio_sppo`.`viagem_conformidade`
  where id_viagem is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


