

with
    autuacao_disciplinar_staging as (
        select
            data_infracao as data,
            datetime(null) as datetime_autuacao,
            id_auto_infracao,
            id_infracao,
            modo,
            servico,
            permissao,
            placa,
            valor,
            data_pagamento,
            date(data) as data_inclusao_stu,
            current_date("America/Sao_Paulo") as data_inclusao_datalake,
            timestamp_captura
        from `rj-smtr`.`monitoramento_staging`.`infracao`
        
            where
                date(data) between date("2022-01-01T00:00:00") and date(
                    "2022-01-01T01:00:00"
                )

        
    ),
    aux_data_inclusao as (
        select
            id_auto_infracao,
            min(data_inclusao_stu) as data_inclusao_stu,
            min(data_inclusao_datalake) as data_inclusao_datalake
        from
            
                (
                    select id_auto_infracao, data_inclusao_stu, data_inclusao_datalake
                    from `rj-smtr`.`monitoramento`.`autuacao_disciplinar_historico`
                    union all
                    select id_auto_infracao, data_inclusao_stu, data_inclusao_datalake
                    from autuacao_disciplinar_staging
                )
            
        group by 1
    ),
    dados_novos as (
        select
            ad.* except (data_inclusao_stu, data_inclusao_datalake, timestamp_captura),
            di.data_inclusao_stu,
            di.data_inclusao_datalake,
            '' as versao,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
            'c58b243d-9dc2-4c60-af65-e26f9910de07' as id_execucao_dbt
        from autuacao_disciplinar_staging ad
        join aux_data_inclusao di using (id_auto_infracao)
        qualify
            row_number() over (
                partition by id_auto_infracao order by timestamp_captura desc
            )
            = 1
    )

        ,
        dados_completos as (
            select *, 'tratada' as fonte, 0 as ordem
            from `rj-smtr`.`monitoramento`.`autuacao_disciplinar_historico`
            union all by name
            select *, 'staging' as fonte, 1 as ordem
            from dados_novos
        ),
        dados_completos_sha as (
            

            select
                *,
                sha256(
                    concat(
                        
                            ifnull(cast(data as string), 'n/a')
                            , 
                        
                            ifnull(cast(datetime_autuacao as string), 'n/a')
                            , 
                        
                            ifnull(cast(id_auto_infracao as string), 'n/a')
                            , 
                        
                            ifnull(cast(id_infracao as string), 'n/a')
                            , 
                        
                            ifnull(cast(modo as string), 'n/a')
                            , 
                        
                            ifnull(cast(servico as string), 'n/a')
                            , 
                        
                            ifnull(cast(permissao as string), 'n/a')
                            , 
                        
                            ifnull(cast(placa as string), 'n/a')
                            , 
                        
                            ifnull(cast(valor as string), 'n/a')
                            , 
                        
                            ifnull(cast(data_pagamento as string), 'n/a')
                            , 
                        
                            ifnull(cast(data_inclusao_stu as string), 'n/a')
                            , 
                        
                            ifnull(cast(data_inclusao_datalake as string), 'n/a')
                            
                        
                    )
                ) as sha_dado
            from dados_completos
        )
    select * except (sha_dado)
    from dados_completos_sha
    qualify
        (
            fonte = 'tratada'
            and (
                lead(sha_dado) over (win) = sha_dado
                or lead(sha_dado) over (win) is null
            )
        )
        or (
            fonte = 'staging'
            and (
                lag(sha_dado) over (win) != sha_dado or lag(sha_dado) over (win) is null
            )
        )
    window win as (partition by id_auto_infracao order by ordem)
