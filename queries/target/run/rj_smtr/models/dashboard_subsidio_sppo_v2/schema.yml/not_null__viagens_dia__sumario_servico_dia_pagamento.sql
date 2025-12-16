select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select viagens_dia
from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`dashboard_subsidio_sppo_v2`.`sumario_servico_dia_pagamento` where data between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00') and data < date('2025-01-05'))
where viagens_dia is null



      
    ) dbt_internal_test