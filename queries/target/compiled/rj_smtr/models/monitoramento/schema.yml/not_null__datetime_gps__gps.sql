
    
    



select datetime_gps
from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`monitoramento`.`gps_onibus_conecta` where data between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00'))
where datetime_gps is null


