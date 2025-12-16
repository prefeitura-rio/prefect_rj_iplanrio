select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select feed_start_date
from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada` where data between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00'))
where feed_start_date is null



      
    ) dbt_internal_test