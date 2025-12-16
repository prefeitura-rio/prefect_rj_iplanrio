select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select codigo_tecnologia
from 
    
        
        

        

        
        (select * from `rj-smtr`.`planejamento`.`tecnologia_servico` where 1=1)
where codigo_tecnologia is null



      
    ) dbt_internal_test