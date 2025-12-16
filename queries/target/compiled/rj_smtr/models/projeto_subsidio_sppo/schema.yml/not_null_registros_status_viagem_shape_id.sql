
    
    



select shape_id
from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`projeto_subsidio_sppo`.`registros_status_viagem` where data between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00'))
where shape_id is null


