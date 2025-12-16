select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      




    with grouped_expression as (
    select
        
        
    
  
( 1=1 and max(data_ordem) >= current_date('America/Sao_Paulo')
)
 as expression


    from 
    
        
        

        

        
        (select * from `rj-smtr`.`br_rj_riodejaneiro_bilhetagem`.`ordem_pagamento_consorcio_operador_dia` where 1=1)
    

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