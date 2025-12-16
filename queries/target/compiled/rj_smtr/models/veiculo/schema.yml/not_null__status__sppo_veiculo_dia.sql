
    
    



select status
from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`veiculo`.`sppo_veiculo_dia` where data between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00'))
where status is null


