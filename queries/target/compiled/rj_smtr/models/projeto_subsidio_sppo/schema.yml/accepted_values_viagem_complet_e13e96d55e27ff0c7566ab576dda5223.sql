
    
    

with all_values as (

    select
        tipo_viagem as value_field,
        count(*) as n_records

    from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`projeto_subsidio_sppo`.`viagem_completa` where data between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00'))
    group by tipo_viagem

)

select *
from all_values
where value_field not in (
    'Completa linha correta','Completa linha incorreta'
)


