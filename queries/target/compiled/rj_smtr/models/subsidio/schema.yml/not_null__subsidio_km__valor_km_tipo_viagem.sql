
    
    



select subsidio_km
from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`subsidio`.`valor_km_tipo_viagem` where data_inicio between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00') or data_fim between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00'))
where subsidio_km is null


