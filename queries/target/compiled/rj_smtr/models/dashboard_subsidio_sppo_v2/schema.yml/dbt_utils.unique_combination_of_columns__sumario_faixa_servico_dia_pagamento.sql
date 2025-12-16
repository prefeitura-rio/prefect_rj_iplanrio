





with validation_errors as (

    select
        data, servico, faixa_horaria_inicio
    from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`dashboard_subsidio_sppo_v2`.`sumario_faixa_servico_dia_pagamento` where data between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00'))
    group by data, servico, faixa_horaria_inicio
    having count(*) > 1

)

select *
from validation_errors


