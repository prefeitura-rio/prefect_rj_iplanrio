
    
    

with dbt_test__target as (

  select id as unique_field
  from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`dbt`.`my_first_dbt_model` where data between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00'))
  where id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


