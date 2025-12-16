
  
    

    create or replace table `rj-smtr-dev`.`caiorogerio__teste`.`teste`
      
    
    

    OPTIONS(
      description=""""""
    )
    as (
      select 
    1 as id, 'teste' as dataset_id, 3 as random_value
union all
select 
    2 as id, 'teste' as dataset_id, 5 as random_value
union all
select
    3 as id, 'teste' as dataset_id, 7 as random_value
union all
select
    4 as id, 'teste' as dataset_id, 9 as random_valueS
    );
  