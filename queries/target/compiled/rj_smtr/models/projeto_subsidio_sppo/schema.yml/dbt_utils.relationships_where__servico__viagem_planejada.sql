




with left_table as (

  select
    servico as id

  from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada` where data between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00'))

  where servico is not null
    and 1=1

),

right_table as (

  select
    servico as id

  from `rj-smtr`.`planejamento`.`tecnologia_servico`

  where servico is not null
    and inicio_vigencia <= date('2022-01-01T01:00:00') and (fim_vigencia is null OR fim_vigencia >= date('2022-01-01T00:00:00'))

),

exceptions as (

  select
    left_table.id,
    right_table.id as right_id

  from left_table

  left join right_table
         on left_table.id = right_table.id

  where right_table.id is null

)

select * from exceptions

