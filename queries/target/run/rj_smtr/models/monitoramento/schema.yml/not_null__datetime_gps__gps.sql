select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select datetime_gps
from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`monitoramento`.`gps_onibus_conecta` where data between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00'))
where datetime_gps is null



      
    ) dbt_internal_test