select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      



select
    1
from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`dashboard_subsidio_sppo_v2`.`sumario_faixa_servico_dia_pagamento` where data between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00'))

where not((valor_a_pagar - valor_penalidade) IS NOT NULL AND (valor_a_pagar - valor_penalidade) >= 0)


      
    ) dbt_internal_test