select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select valor_subsidio_pago
from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`dashboard_subsidio_sppo`.`sumario_servico_dia` where data between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00') and data < date('2024-08-16'))
where valor_subsidio_pago is null



      
    ) dbt_internal_test