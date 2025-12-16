with
        viagem_completa as (
            select data, datetime_ultima_atualizacao, feed_start_date
            from
                -- rj-smtr.projeto_subsidio_sppo.viagem_completa
                `rj-smtr`.`projeto_subsidio_sppo`.`viagem_completa`
            left join `rj-smtr`.`projeto_subsidio_sppo`.`subsidio_data_versao_efetiva` using (data)
            where
                data >= "2024-04-01"
                and data between date("2022-01-01T01:00:00") and date(
                    "2022-01-01T01:00:00"
                )
        ),
        sumario_servico_dia_historico as (
            select data, datetime_ultima_atualizacao
            from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`dashboard_subsidio_sppo`.`sumario_servico_dia_historico` where data between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00') and data < date('2024-08-16'))
            where
                data between date("2022-01-01T01:00:00") and date(
                    "2022-01-01T01:00:00"
                )
        ),
        feed_info as (
            select feed_start_date, feed_update_datetime
            from `rj-smtr`.`gtfs`.`feed_info`
        )
    select distinct data
    from viagem_completa as c
    left join sumario_servico_dia_historico as h using (data)
    left join feed_info as f using (feed_start_date)
    where
        c.datetime_ultima_atualizacao > h.datetime_ultima_atualizacao
        and c.datetime_ultima_atualizacao > f.feed_update_datetime