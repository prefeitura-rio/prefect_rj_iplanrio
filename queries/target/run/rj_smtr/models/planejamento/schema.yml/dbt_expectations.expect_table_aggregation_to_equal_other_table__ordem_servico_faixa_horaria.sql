select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    with a as (
        
    select
        
        feed_start_date as col_1,
        tipo_os as col_2,
        
        count(distinct tipo_os) as expression
    from
        
    
        
        

        

        
        (select * from `rj-smtr`.`planejamento`.`ordem_servico_faixa_horaria` where 1=1)
    where
        feed_start_date = '2024-12-31'
    
    
    group by
        1,
        2
        
    

    ),
    b as (
        
    select
        
        feed_start_date as col_1,
        tipo_os as col_2,
        
        count(distinct tipo_os) as expression
    from
        `rj-smtr`.`gtfs`.`ordem_servico`
    where
        feed_start_date = '2024-12-31'
    
    
    group by
        1,
        2
        
    

    ),
    final as (

        select
            coalesce(a.col_1, b.col_1) as col_1,
            coalesce(a.col_2, b.col_2) as col_2,
            
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
            a.col_2 = b.col_2 
            
    )
    -- DEBUG:
    -- select * from final
    select
        *
    from final
    where
        
        expression_difference > 0.0
        
      
    ) dbt_internal_test