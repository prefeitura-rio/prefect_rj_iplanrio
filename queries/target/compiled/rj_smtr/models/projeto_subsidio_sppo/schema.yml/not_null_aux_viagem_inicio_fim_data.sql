
    
    



select data
from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`projeto_subsidio_sppo`.`aux_viagem_inicio_fim` where data between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00'))
where data is null


