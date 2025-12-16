select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select inicio_vigencia
from 
    
        
        

        

        
        (select * from `rj-smtr`.`planejamento`.`tecnologia_servico` where 1=1)
where inicio_vigencia is null



      
    ) dbt_internal_test