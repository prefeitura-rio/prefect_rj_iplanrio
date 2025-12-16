





    

    

    

    

    
    

    
    




with
    licenciamento_staging as (
        select
            date(data) as data,
            current_date("America/Sao_Paulo") as data_processamento,
            id_veiculo,
            placa,
            modo,
            permissao,
            ano_fabricacao,
            id_carroceria,
            id_interno_carroceria,
            carroceria,
            id_chassi,
            id_fabricante_chassi,
            nome_chassi,
            id_planta,
            tipo_veiculo,
            status,
            data_inicio_vinculo,
            data_ultima_vistoria,
            ano_ultima_vistoria,
            ultima_situacao,
            case
                when tipo_veiculo like "%BASIC%" or tipo_veiculo like "%BS%"
                then "BASICO"
                when tipo_veiculo like "%MIDI%"
                then "MIDI"
                when tipo_veiculo like "%MINI%"
                then "MINI"
                when tipo_veiculo like "%PDRON%" or tipo_veiculo like "%PADRON%"
                then "PADRON"
                when tipo_veiculo like "%ARTICULADO%"
                then "ARTICULADO"
            end as tecnologia,
            quantidade_lotacao_pe,
            quantidade_lotacao_sentado,
            tipo_combustivel,
            indicador_ar_condicionado,
            indicador_elevador,
            indicador_usb,
            indicador_wifi,
            lag(date(data)) over (win) as ultima_data,
            min(date(data)) over (win) as primeira_data,
            date(data) as data_arquivo_fonte
        from `rj-smtr`.`cadastro_staging`.`licenciamento_stu`
        where
            data > '2025-03-31'
            
                and date(data) between date('2022-01-01') and date(
                    "2022-01-01T01:00:00"
                )
            
        window win as (partition by data, id_veiculo, placa order by data)
    ),
    datas_faltantes as (
        select distinct
            data,
            max(l.data_arquivo_fonte) over (
                order by data rows unbounded preceding
            ) as data_arquivo_fonte
        from
            unnest(
                
                    generate_date_array(
                        date('2022-01-01T00:00:00'),
                        date('2022-01-01T01:00:00'),
                        interval 1 day
                    )
                
            ) as data
        full outer join licenciamento_staging l using (data)
        where data > '2025-03-31'
    ),
    licenciamento_datas_preenchidas as (
        select df.data, l.* except (data)
        from licenciamento_staging l
        left join datas_faltantes df using (data_arquivo_fonte)
    ),
    veiculo_fiscalizacao_lacre as (
        select * from `rj-smtr`.`monitoramento`.`veiculo_fiscalizacao_lacre`
    ),
    inicio_vinculo_preenchido as (
        select data_corrigida as data, s.* except (data)
        from
            licenciamento_staging s,
            unnest(
                generate_date_array(s.data_inicio_vinculo, data, interval 1 day)
            ) as data_corrigida
        where
            s.ultima_data is null
            and s.data != s.primeira_data
            and data_corrigida > '2025-03-31'
    ),
    dados_novos as (
        select *
        from inicio_vinculo_preenchido
        union all
        select *
        from licenciamento_datas_preenchidas
    ),
    
        dados_atuais as (
            select *
            from `rj-smtr`.`cadastro`.`veiculo_licenciamento_dia`
            where
                data between date("2022-01-01T00:00:00") and date(
                    "2022-01-01T01:00:00"
                )
                
                
                
        ),
        dados_novos_lacre_vistoria as (
            select * except (ultima_data, primeira_data)
            from dados_novos

            
        )
    ,
    veiculo_lacrado as (
        select
            dn.*,
            vfl.id_veiculo is not null as indicador_veiculo_lacrado,
            '' as versao,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
        from dados_novos_lacre_vistoria dn
        left join
            veiculo_fiscalizacao_lacre vfl
            on dn.id_veiculo = vfl.id_veiculo
            and dn.placa = vfl.placa
            and dn.data >= vfl.data_inicio_lacre
            and (dn.data < vfl.data_fim_lacre or vfl.data_fim_lacre is null)
    ),
    veiculo_lacrado_deduplicado as (
        select *
        from veiculo_lacrado
        qualify
            row_number() over (
                partition by data, id_veiculo, placa
                order by indicador_veiculo_lacrado desc
            )
            = 1
    ),
    data_vistoria_atualizacao as (
        select
            date(data) as data,
            id_veiculo,
            placa,
            data_ultima_vistoria,
            ano_ultima_vistoria
        from dados_novos
        qualify
            (
                lag(data_ultima_vistoria) over (win) is null
                and data_ultima_vistoria is not null
            )
            or lag(data_ultima_vistoria) over (win) != data_ultima_vistoria
        window win as (partition by id_veiculo, placa order by data)

    ),
    nova_data_ultima_vistoria as (
        select
            nova_data as data,
            id_veiculo,
            placa,
            data_ultima_vistoria,
            ano_ultima_vistoria
        from
            data_vistoria_atualizacao d,
            unnest(
                generate_date_array(data_ultima_vistoria, data, interval 1 day)
            ) as nova_data
        qualify
            row_number() over (
                partition by nova_data, id_veiculo, placa order by d.data desc
            )
            = 1
    ),
    veiculo_vistoriado as (
        select
            v.* except (data_ultima_vistoria, ano_ultima_vistoria),
            ifnull(
                d.data_ultima_vistoria, v.data_ultima_vistoria
            ) as data_ultima_vistoria,
            ifnull(d.ano_ultima_vistoria, v.ano_ultima_vistoria) as ano_ultima_vistoria
        from veiculo_lacrado_deduplicado v
        left join nova_data_ultima_vistoria d using (data, id_veiculo, placa)
    ),
    final as (
        

            with
                dados_completos as (
                    select *, 1 as priority
                    from dados_atuais

                    union all by name

                    select *, cast(null as string) as id_execucao_dbt, 0 as priority
                    from veiculo_vistoriado
                ),
                dados_completos_sha as (
                    

                    select
                        *,
                        sha256(
                            concat(
                                
                                    ifnull(cast(data as string), 'n/a')
                                    , 
                                
                                    ifnull(cast(id_veiculo as string), 'n/a')
                                    , 
                                
                                    ifnull(cast(placa as string), 'n/a')
                                    , 
                                
                                    ifnull(cast(modo as string), 'n/a')
                                    , 
                                
                                    ifnull(cast(permissao as string), 'n/a')
                                    , 
                                
                                    ifnull(cast(ano_fabricacao as string), 'n/a')
                                    , 
                                
                                    ifnull(cast(id_carroceria as string), 'n/a')
                                    , 
                                
                                    ifnull(cast(id_interno_carroceria as string), 'n/a')
                                    , 
                                
                                    ifnull(cast(carroceria as string), 'n/a')
                                    , 
                                
                                    ifnull(cast(id_chassi as string), 'n/a')
                                    , 
                                
                                    ifnull(cast(id_fabricante_chassi as string), 'n/a')
                                    , 
                                
                                    ifnull(cast(nome_chassi as string), 'n/a')
                                    , 
                                
                                    ifnull(cast(id_planta as string), 'n/a')
                                    , 
                                
                                    ifnull(cast(tipo_veiculo as string), 'n/a')
                                    , 
                                
                                    ifnull(cast(status as string), 'n/a')
                                    , 
                                
                                    ifnull(cast(data_inicio_vinculo as string), 'n/a')
                                    , 
                                
                                    ifnull(cast(data_ultima_vistoria as string), 'n/a')
                                    , 
                                
                                    ifnull(cast(ano_ultima_vistoria as string), 'n/a')
                                    , 
                                
                                    ifnull(cast(ultima_situacao as string), 'n/a')
                                    , 
                                
                                    ifnull(cast(tecnologia as string), 'n/a')
                                    , 
                                
                                    ifnull(cast(quantidade_lotacao_pe as string), 'n/a')
                                    , 
                                
                                    ifnull(cast(quantidade_lotacao_sentado as string), 'n/a')
                                    , 
                                
                                    ifnull(cast(tipo_combustivel as string), 'n/a')
                                    , 
                                
                                    ifnull(cast(indicador_ar_condicionado as string), 'n/a')
                                    , 
                                
                                    ifnull(cast(indicador_elevador as string), 'n/a')
                                    , 
                                
                                    ifnull(cast(indicador_usb as string), 'n/a')
                                    , 
                                
                                    ifnull(cast(indicador_wifi as string), 'n/a')
                                    , 
                                
                                    ifnull(cast(indicador_veiculo_lacrado as string), 'n/a')
                                    , 
                                
                                    ifnull(cast(data_arquivo_fonte as string), 'n/a')
                                    
                                
                            )
                        ) as sha_dado
                    from dados_completos
                ),
                dados_completos_invocation_id as (
                    select
                        * except (id_execucao_dbt),
                        case
                            when
                                lag(sha_dado) over (win) != sha_dado
                                or (
                                    lag(sha_dado) over (win) is null
                                    and count(*) over (win) = 1
                                )
                            then 'c58b243d-9dc2-4c60-af65-e26f9910de07'
                            else
                                ifnull(id_execucao_dbt, lag(id_execucao_dbt) over (win))
                        end as id_execucao_dbt
                    from dados_completos_sha
                    window
                        win as (
                            partition by data, id_veiculo, placa, data_processamento
                            order by priority desc
                        )

                ),
                dados_completos_deduplicados as (
                    select *
                    from dados_completos_invocation_id
                    qualify
                        row_number() over (
                            partition by data, data_processamento, id_veiculo, placa
                            order by priority
                        )
                        = 1
                )
            select * except (sha_dado)
            from dados_completos_deduplicados
            qualify
                lag(sha_dado) over (win) != sha_dado or lag(sha_dado) over (win) is null
            window
                win as (
                    partition by data, id_veiculo, placa order by data_processamento
                )
        
    )
select
    data,
    data_processamento,
    id_veiculo,
    placa,
    modo,
    permissao,
    ano_fabricacao,
    id_carroceria,
    id_interno_carroceria,
    carroceria,
    id_chassi,
    id_fabricante_chassi,
    nome_chassi,
    id_planta,
    tipo_veiculo,
    status,
    data_inicio_vinculo,
    data_ultima_vistoria,
    ano_ultima_vistoria,
    ultima_situacao,
    tecnologia,
    quantidade_lotacao_pe,
    quantidade_lotacao_sentado,
    tipo_combustivel,
    indicador_ar_condicionado,
    indicador_elevador,
    indicador_usb,
    indicador_wifi,
    indicador_veiculo_lacrado,
    data_arquivo_fonte,
    versao,
    datetime_ultima_atualizacao,
    id_execucao_dbt
from final
where data <= data_processamento