
    with a as (
        
    select
        
        feed_start_date as col_1,
        tipo_os as col_2,
        tipo_dia as col_3,
        servico as col_4,
        faixa_horaria_inicio as col_5,
        
        count(distinct feed_start_date) as expression
    from
        
    
        
        

        

        
        (select * from `rj-smtr`.`gtfs`.`ordem_servico_trips_shapes` where 1=1)
    where
        feed_start_date = '2024-12-31'
    
    
    group by
        1,
        2,
        3,
        4,
        5
        
    

    ),
    b as (
        
    select
        
        feed_start_date as col_1,
        tipo_os as col_2,
        tipo_dia as col_3,
        servico as col_4,
        faixa_horaria_inicio as col_5,
        
        count(distinct feed_start_date) as expression
    from
        `rj-smtr`.`planejamento`.`ordem_servico_faixa_horaria`
    where
        feed_start_date = '2024-12-31' AND (quilometragem != 0 AND (partidas != 0 OR partidas IS NULL))
    
    
    group by
        1,
        2,
        3,
        4,
        5
        
    

    ),
    final as (

        select
            coalesce(a.col_1, b.col_1) as col_1,
            coalesce(a.col_2, b.col_2) as col_2,
            coalesce(a.col_3, b.col_3) as col_3,
            coalesce(a.col_4, b.col_4) as col_4,
            coalesce(a.col_5, b.col_5) as col_5,
            
            a.expression,
            b.expression as compare_expression,
            abs(coalesce(a.expression, 0) - coalesce(b.expression, 0)) as expression_difference,
            abs(coalesce(a.expression, 0) - coalesce(b.expression, 0))/
                nullif(a.expression * 1.0, 0) as expression_difference_percent
        from
        
            a
            full outer join
            b on
            a.col_1 = b.col_1 and
            a.col_2 = b.col_2 and
            a.col_3 = b.col_3 and
            a.col_4 = b.col_4 and
            a.col_5 = b.col_5 
            
    )
    -- DEBUG:
    -- select * from final
    select
        *
    from final
    where
        
        expression_difference > 0.0
        