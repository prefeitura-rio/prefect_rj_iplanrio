






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and data > '2024-08-15' and data < '2025-01-05'
)
 as expression


    from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`financeiro`.`subsidio_sumario_servico_dia_pagamento` where data between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00'))
    

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







