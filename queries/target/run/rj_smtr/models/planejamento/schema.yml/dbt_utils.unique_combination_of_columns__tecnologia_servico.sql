select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      





with validation_errors as (

    select
        inicio_vigencia, servico
    from 
    
        
        

        

        
        (select * from `rj-smtr`.`planejamento`.`tecnologia_servico` where 1=1)
    group by inicio_vigencia, servico
    having count(*) > 1

)

select *
from validation_errors



      
    ) dbt_internal_test