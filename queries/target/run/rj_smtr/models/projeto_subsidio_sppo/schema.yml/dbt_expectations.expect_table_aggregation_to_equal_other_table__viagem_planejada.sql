select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    with a as (
        
    select
        
        tipo_dia as col_1,
        
        count(distinct tipo_dia) as expression
    from
        
    
        
        

        

        
        (select * from `rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada` where 1=1)
    where
        DATA BETWEEN DATE('2022-01-01T00:00:00') AND DATE('2022-01-01T01:00:00')
    
    
    group by
        1
        
    

    ),
    b as (
        
    select
        
        coalesce(concat(concat(tipo_dia, " - "), subtipo_dia), tipo_dia) as col_1,
        
        count(distinct coalesce(concat(concat(tipo_dia, " - "), subtipo_dia), tipo_dia)) as expression
    from
        `rj-smtr`.`projeto_subsidio_sppo`.`subsidio_data_versao_efetiva`
    where
        DATA BETWEEN DATE('2022-01-01T00:00:00') AND DATE('2022-01-01T01:00:00')
    
    
    group by
        1
        
    

    ),
    final as (

        select
            coalesce(a.col_1, b.col_1) as col_1,
            
            a.expression,
            b.expression as compare_expression,
            abs(coalesce(a.expression, 0) - coalesce(b.expression, 0)) as expression_difference,
            abs(coalesce(a.expression, 0) - coalesce(b.expression, 0))/
                nullif(a.expression * 1.0, 0) as expression_difference_percent
        from
        
            a
            full outer join
            b on
            a.col_1 = b.col_1 
            
    )
    -- DEBUG:
    -- select * from final
    select
        *
    from final
    where
        
        expression_difference > 0.0
        
      
    ) dbt_internal_test