select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      





with validation_errors as (

    select
        data, servico, sentido, faixa_horaria_inicio, shape_id
    from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada` where data between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00'))
    group by data, servico, sentido, faixa_horaria_inicio, shape_id
    having count(*) > 1

)

select *
from validation_errors



      
    ) dbt_internal_test