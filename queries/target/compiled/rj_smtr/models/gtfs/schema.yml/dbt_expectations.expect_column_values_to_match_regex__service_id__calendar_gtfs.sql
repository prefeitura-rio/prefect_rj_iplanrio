




    with grouped_expression as (
    select
        
        
    
  


    
regexp_instr(service_id, '^([USD]_|EXCEP)', 1, 1)


 > 0
 as expression


    from 
    
        
        

        

        
        (select * from `rj-smtr`.`gtfs`.`calendar` where feed_start_date = '2024-12-31')
    

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




