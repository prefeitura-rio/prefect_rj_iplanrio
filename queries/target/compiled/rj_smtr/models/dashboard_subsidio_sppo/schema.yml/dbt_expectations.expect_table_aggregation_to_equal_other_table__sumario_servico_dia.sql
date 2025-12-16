
    with a as (
        
    select
        
        valor_penalidade as col_1,
        
        count(distinct valor_penalidade) as expression
    from
        
    
        
        

        

        
        (select * from `rj-smtr`.`dashboard_subsidio_sppo`.`sumario_servico_dia` where 1=1)
    where
        DATA BETWEEN DATE('2022-01-01T00:00:00') AND DATE('2022-01-01T01:00:00') AND valor_penalidade IS NOT null AND valor_penalidade != 0
    
    
    group by
        1
        
    

    ),
    b as (
        
    select
        
        -valor as col_1,
        
        count(distinct valor) as expression
    from
        `rj-smtr`.`dashboard_subsidio_sppo`.`valor_tipo_penalidade`
    where
        valor IS NOT null AND valor != 0
    
    
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
        