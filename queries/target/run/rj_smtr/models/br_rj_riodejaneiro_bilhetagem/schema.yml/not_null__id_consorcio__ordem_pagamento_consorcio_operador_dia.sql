select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select id_consorcio
from 
    
        
        

        

        
            
            
            
            
        
        (select * from `rj-smtr`.`br_rj_riodejaneiro_bilhetagem`.`ordem_pagamento_consorcio_operador_dia` where data_ordem in ())
where id_consorcio is null



      
    ) dbt_internal_test