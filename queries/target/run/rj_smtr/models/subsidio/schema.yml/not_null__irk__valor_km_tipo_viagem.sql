select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select irk
from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`subsidio`.`valor_km_tipo_viagem` where data_inicio between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00') or data_fim between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00'))
where irk is null



      
    ) dbt_internal_test