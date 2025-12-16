



    with grouped_expression as (
    select
        
        feed_start_date as col_1,
        tipo_dia as col_2,
        tipo_os as col_3,
        servico as col_4,
        faixa_horaria_inicio as col_5,
        shape_id as col_6,
        
        
    
  
( 1=1 and count(*) >= 1 and count(*) <= 2
)
 as expression


    from 
    
        
        

        

        
        (select * from `rj-smtr`.`gtfs`.`ordem_servico_trips_shapes` where 1=1)
    where
        feed_start_date = '2024-12-31'
    
    
    group by
    feed_start_date,
    tipo_dia,
    tipo_os,
    servico,
    faixa_horaria_inicio,
    shape_id
    
    

),
validation_errors as (

    select
        *
    from
        grouped_expression
    where
        not(expression = true)

)

select *
from validation_errors





