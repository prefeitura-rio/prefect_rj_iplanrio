

with meet_condition as(
  select *
  from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`dashboard_subsidio_sppo`.`viagens_remuneradas` where data between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00'))
),

validation_errors as (
  select *
  from meet_condition
  where
    -- never true, defaults to an empty result set. Exists to ensure any combo of the `or` clauses below succeeds
    1 = 2
    -- records with a value >= min_value are permitted. The `not` flips this to find records that don't meet the rule.
    or not subsidio_km >= 0
)

select *
from validation_errors

