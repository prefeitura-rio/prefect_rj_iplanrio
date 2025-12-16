





with validation_errors as (

    select
        data, id_veiculo
    from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`monitoramento`.`veiculo_dia` where data between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00'))
    group by data, id_veiculo
    having count(*) > 1

)

select *
from validation_errors


