select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      with
        base as (select distinct status, data_inicio, data_fim from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`subsidio`.`valor_km_tipo_viagem` where data_inicio between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00') or data_fim between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00'))),
        overlaps as (
            select
                a.status,
                a.data_inicio as a_inicio,
                a.data_fim as a_fim,
                b.data_inicio as b_inicio,
                b.data_fim as b_fim
            from base a
            join
                base b
                on a.status = b.status
                and a.data_inicio < b.data_fim
                and b.data_inicio < a.data_fim
                and a.data_inicio != b.data_inicio
                and a.data_fim != b.data_fim
        )
    select *
    from overlaps
      
    ) dbt_internal_test