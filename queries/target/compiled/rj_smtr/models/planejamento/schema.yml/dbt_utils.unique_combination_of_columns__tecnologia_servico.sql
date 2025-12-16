





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


