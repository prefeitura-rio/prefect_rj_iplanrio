-- depends_on: `rj-smtr`.`monitoramento_staging`.`staging_realocacao_conecta`
                with
                t as (
                    select datetime(timestamp_array) as timestamp_array
                    from
                        unnest(
                            generate_timestamp_array(
                                timestamp("2022-01-01T00:00:00"),
                                timestamp("2022-01-01T01:00:00"),
                                interval 10 minute
                            )
                        ) as timestamp_array
                    where timestamp_array < timestamp("2022-01-01T01:00:00")
                ),
                capture as (
                    select distinct datetime_captura
                    from `rj-smtr`.`monitoramento_staging`.`staging_realocacao_conecta`
                    where
                        (
                            
    
    
    

    
        data = date("2022-01-01T00:00:00")
        and hora between 0 and 1

    


                        )
                ),
                missing_timestamps as (
                    select t.timestamp_array as datetime_captura
                    from t
                    left join capture c on t.timestamp_array = c.datetime_captura
                    where c.datetime_captura is null
                )
            select *
            from missing_timestamps