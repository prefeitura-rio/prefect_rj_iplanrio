select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select id
from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`dbt`.`my_second_dbt_model` where data between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00'))
where id is null



      
    ) dbt_internal_test