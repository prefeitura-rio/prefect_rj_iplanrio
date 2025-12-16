





with validation_errors as (

    select
        datetime_gps, id_veiculo, latitude, longitude
    from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`monitoramento`.`gps_onibus_conecta` where data between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00'))
    group by datetime_gps, id_veiculo, latitude, longitude
    having count(*) > 1

)

select *
from validation_errors


