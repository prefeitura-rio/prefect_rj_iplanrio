
    
    

with all_values as (

    select
        sentido as value_field,
        count(*) as n_records

    from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`projeto_subsidio_sppo`.`aux_viagem_circular` where data between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00'))
    group by sentido

)

select *
from all_values
where value_field not in (
    'I','V','C'
)


