select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      




with left_table as (

  select
    `id_auto_infracao` as id

  from 
    
        
        

        

        
        (select * from `rj-smtr`.`monitoramento`.`veiculo_fiscalizacao_lacre` where 1=1)

  where `id_auto_infracao` is not null
    and 1=1

),

right_table as (

  select
    id_auto_infracao as id

  from `rj-smtr`.`monitoramento`.`autuacao_disciplinar_historico`

  where id_auto_infracao is not null
    and 1=1

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


      
    ) dbt_internal_test