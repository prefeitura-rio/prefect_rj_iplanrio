
    
    



select subsidio_km_teto
from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`dashboard_subsidio_sppo`.`viagens_remuneradas` where data between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00'))
where subsidio_km_teto is null


