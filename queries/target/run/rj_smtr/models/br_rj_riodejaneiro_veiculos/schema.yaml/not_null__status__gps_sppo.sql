select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select status
from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`br_rj_riodejaneiro_veiculos`.`gps_sppo` where data between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00'))
where status is null



      
    ) dbt_internal_test