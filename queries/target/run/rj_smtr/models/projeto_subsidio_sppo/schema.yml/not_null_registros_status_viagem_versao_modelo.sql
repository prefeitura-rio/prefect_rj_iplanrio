select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select versao_modelo
from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`projeto_subsidio_sppo`.`registros_status_viagem` where data between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00'))
where versao_modelo is null



      
    ) dbt_internal_test