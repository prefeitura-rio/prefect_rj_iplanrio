select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and data > '2022-05-31' and data < '2024-08-16'
)
 as expression


    from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`dashboard_subsidio_sppo`.`sumario_servico_tipo_viagem_dia` where data between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00') and data < date('2024-08-16'))
    

),
validation_errors as (

    select
        *
    from
        grouped_expression
    where
        not(expression = true)

)

select *
from validation_errors








      
    ) dbt_internal_test