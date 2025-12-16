select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      





with validation_errors as (

    select
        feed_start_date, tipo_dia, tipo_os, servico, sentido, faixa_horaria_inicio, shape_id
    from 
    
        
        

        

        
        (select * from `rj-smtr`.`gtfs`.`ordem_servico_trips_shapes` where feed_start_date = '2024-12-31')
    group by feed_start_date, tipo_dia, tipo_os, servico, sentido, faixa_horaria_inicio, shape_id
    having count(*) > 1

)

select *
from validation_errors



      
    ) dbt_internal_test