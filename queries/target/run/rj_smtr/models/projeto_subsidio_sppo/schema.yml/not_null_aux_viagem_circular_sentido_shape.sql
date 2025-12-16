select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select sentido_shape
from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`projeto_subsidio_sppo`.`aux_viagem_circular` where data between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00'))
where sentido_shape is null



      
    ) dbt_internal_test