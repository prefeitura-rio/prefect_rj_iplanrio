with
                t as (
                    select datetime(timestamp_array) as timestamp_array
                    from
                        unnest(
                            generate_timestamp_array(
                                timestamp("2022-01-01T00:00:00"),
                                timestamp("2022-01-01T01:00:00"),
                                interval 1 minute
                            )
                        ) as timestamp_array
                    where timestamp_array < timestamp("2022-01-01T01:00:00")
                ),
                logs_table as (
                    select
                        safe_cast(
                            datetime(
                                timestamp(timestamp_captura), "America/Sao_Paulo"
                            ) as datetime
                        ) timestamp_captura,
                        safe_cast(sucesso as boolean) sucesso,
                        safe_cast(erro as string) erro,
                        safe_cast(data as date) data
                    from
                        `rj-smtr-staging.br_rj_riodejaneiro_onibus_gps_staging.registros_logs`
                        as t
                ),
                logs as (
                    select
                        *, timestamp_trunc(timestamp_captura, minute) as timestamp_array
                    from logs_table
                    where
                        data between date(
                            timestamp("2022-01-01T00:00:00")
                        ) and date(timestamp("2022-01-01T01:00:00"))
                        and timestamp_captura
                        between "2022-01-01T00:00:00"
                        and "2022-01-01T01:00:00"
                )
            select
                coalesce(
                    logs.timestamp_captura, t.timestamp_array
                ) as timestamp_captura,
                logs.erro
            from t
            left join logs on logs.timestamp_array = t.timestamp_array
            where logs.sucesso is not true