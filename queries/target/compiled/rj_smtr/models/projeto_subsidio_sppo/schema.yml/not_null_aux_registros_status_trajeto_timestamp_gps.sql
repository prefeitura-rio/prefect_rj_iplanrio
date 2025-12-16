
    
    



select timestamp_gps
from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`projeto_subsidio_sppo`.`aux_registros_status_trajeto` where data between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00'))
where timestamp_gps is null


