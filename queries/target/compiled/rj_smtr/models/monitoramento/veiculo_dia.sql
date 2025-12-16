

with
    licenciamento as (
        select
            *,
            case
                when
                    ano_ultima_vistoria >= cast(
                        extract(
                            year
                            from
                                date_sub(
                                    date(data),
                                    interval 1 year
                                )
                        ) as int64
                    )
                then true  -- Última vistoria realizada dentro do período válido
                when
                    data_ultima_vistoria is null
                    and date_diff(date(data), data_inicio_vinculo, day)
                    <= 15
                then true  -- Caso o veículo seja novo, existe a tolerância de 15 dias para a primeira vistoria
                else false
            end as indicador_vistoriado,
        from `rj-smtr`.`cadastro`.`veiculo_licenciamento_dia`
        where
            (
                data_processamento <= date_add(data, interval 7 day)
                or data_processamento = '2025-06-25'
            )
            
                and data between date("2022-01-01T00:00:00") and date(
                    "2022-01-01T01:00:00"
                )

            
        qualify
            row_number() over (
                partition by data, id_veiculo, placa order by data_processamento desc
            )
            = 1
    ),
    autuacao_disciplinar as (
        select *
        from `rj-smtr`.`monitoramento`.`autuacao_disciplinar_historico`
        where
            data_inclusao_datalake <= date_add(data, interval 7 day)
            
                and data between date("2022-01-01T00:00:00") and date(
                    "2022-01-01T01:00:00"
                )

            
    ),
    gps as (
        select *
        from `rj-smtr`.`monitoramento_staging`.`aux_veiculo_gps_dia`
        
            where
                data between date("2022-01-01T00:00:00") and date(
                    "2022-01-01T01:00:00"
                )

        
    ),
    registros_agente_verao as (
        select distinct data, id_veiculo
        from `rj-smtr`.`veiculo`.`sppo_registro_agente_verao`
        
        
            where
                data between date("2022-01-01T00:00:00") and date(
                    "2022-01-01T01:00:00"
                )
        
    ),
    gps_licenciamento as (
        select
            data,
            id_veiculo,
            l.placa,
            l.modo,
            l.tecnologia,
            l.tipo_veiculo,
            l.id_veiculo is not null as indicador_licenciado,
            l.indicador_vistoriado,
            l.indicador_ar_condicionado,
            l.indicador_veiculo_lacrado,
            r.id_veiculo is not null as indicador_registro_agente_verao_ar_condicionado,
            l.data_processamento as data_processamento_licenciamento,
            r.data as data_registro_agente_verao
        from gps g
        left join licenciamento as l using (data, id_veiculo)
        left join registros_agente_verao as r using (data, id_veiculo)
    ),
    autuacao_ar_condicionado as (
        select
            data,
            placa,
            data_inclusao_datalake as data_inclusao_datalake_autuacao_ar_condicionado
        from autuacao_disciplinar
        where id_infracao = "023.II"
    ),
    autuacao_completa as (
        select distinct
            data,
            placa,
            ac.data is not null as indicador_autuacao_ar_condicionado,
            ac.data_inclusao_datalake_autuacao_ar_condicionado,
        from autuacao_ar_condicionado ac
    ),
    gps_licenciamento_autuacao as (
        select
            data,
            gl.id_veiculo,
            placa,
            gl.modo,
            gl.tecnologia,
            gl.tipo_veiculo,
            struct(
                struct(
                    gl.indicador_licenciado as valor,
                    gl.data_processamento_licenciamento
                    as data_processamento_licenciamento
                ) as indicador_licenciado,
                struct(
                    gl.indicador_vistoriado as valor,
                    gl.data_processamento_licenciamento
                    as data_processamento_licenciamento
                ) as indicador_vistoriado,
                struct(
                    gl.indicador_ar_condicionado as valor,
                    gl.data_processamento_licenciamento
                    as data_processamento_licenciamento
                ) as indicador_ar_condicionado,
                struct(
                    gl.indicador_veiculo_lacrado as valor,
                    gl.data_processamento_licenciamento
                    as data_processamento_licenciamento
                ) as indicador_veiculo_lacrado,
                struct(
                    a.indicador_autuacao_ar_condicionado as valor,
                    a.data_inclusao_datalake_autuacao_ar_condicionado
                    as data_inclusao_datalake_autuacao
                ) as indicador_autuacao_ar_condicionado,
                struct(
                    gl.indicador_registro_agente_verao_ar_condicionado as valor,
                    gl.data_registro_agente_verao as data_registro_agente_verao

                ) as indicador_registro_agente_verao_ar_condicionado
            ) as indicadores
        from gps_licenciamento gl
        left join autuacao_completa as a using (data, placa)
    ),
    dados_novos as (
        select
            data,
            id_veiculo,
            placa,
            modo,
            tecnologia,
            tipo_veiculo,
            case
                when indicadores.indicador_licenciado.valor is false
                then "Não licenciado"
                when indicadores.indicador_vistoriado.valor is false
                then "Não vistoriado"
                when indicadores.indicador_veiculo_lacrado.valor is true
                then "Lacrado"
                when
                    indicadores.indicador_ar_condicionado.valor is true
                    and indicadores.indicador_autuacao_ar_condicionado.valor is true
                then "Autuado por ar inoperante"
                when
                    indicadores.indicador_autuacao_ar_condicionado.valor is true
                    and indicadores.indicador_registro_agente_verao_ar_condicionado.valor
                    is true
                then "Registrado com ar inoperante"
                when indicadores.indicador_ar_condicionado.valor is false
                then "Licenciado sem ar e não autuado"
                when indicadores.indicador_ar_condicionado.valor is true
                then "Licenciado com ar e não autuado"
            end as status,
            to_json(indicadores) as indicadores,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
            "" as versao,
            'c58b243d-9dc2-4c60-af65-e26f9910de07' as id_execucao_dbt
        from gps_licenciamento_autuacao
    )

        ,
        dados_completos as (
            select *, 1 as ordem
            from dados_novos

            union all

            select *, 0 as ordem
            from `rj-smtr`.`monitoramento`.`veiculo_dia`
            where
                data between date("2022-01-01T00:00:00") and date(
                    "2022-01-01T01:00:00"
                )

        ),
        sha_dados as (
            

            select
                *,
                sha256(
                    concat(
                        
                            ifnull(
                                cast(data as string)
                                ,
                                'n/a'
                            )
                            , 
                        
                            ifnull(
                                cast(id_veiculo as string)
                                ,
                                'n/a'
                            )
                            , 
                        
                            ifnull(
                                cast(placa as string)
                                ,
                                'n/a'
                            )
                            , 
                        
                            ifnull(
                                cast(modo as string)
                                ,
                                'n/a'
                            )
                            , 
                        
                            ifnull(
                                cast(tecnologia as string)
                                ,
                                'n/a'
                            )
                            , 
                        
                            ifnull(
                                cast(tipo_veiculo as string)
                                ,
                                'n/a'
                            )
                            , 
                        
                            ifnull(
                                cast(status as string)
                                ,
                                'n/a'
                            )
                            , 
                        
                            ifnull(
                                to_json_string(indicadores)
                                ,
                                'n/a'
                            )
                            
                        
                    )
                ) as sha_dado
            from dados_completos
        ),
        dados_completos_invocation_id as (
            select
                * except (id_execucao_dbt, sha_dado),
                case
                    when
                        lag(sha_dado) over (win) != sha_dado
                        or (lag(sha_dado) over (win) is null and ordem = 1)
                    then id_execucao_dbt
                    else lag(id_execucao_dbt) over (win)
                end as id_execucao_dbt
            from sha_dados
            window win as (partition by data, id_veiculo order by ordem)
        )

    select * except (ordem)
    from dados_completos_invocation_id
    where ordem = 1
