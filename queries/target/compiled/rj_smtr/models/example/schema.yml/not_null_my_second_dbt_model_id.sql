
    
    



select id
from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`dbt`.`my_second_dbt_model` where data between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00'))
where id is null


