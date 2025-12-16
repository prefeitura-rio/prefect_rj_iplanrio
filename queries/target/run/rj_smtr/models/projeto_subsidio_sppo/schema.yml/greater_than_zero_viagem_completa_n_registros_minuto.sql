select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      

    select *
    from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`projeto_subsidio_sppo`.`viagem_completa` where data between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00'))
    where n_registros_minuto <= 0


      
    ) dbt_internal_test