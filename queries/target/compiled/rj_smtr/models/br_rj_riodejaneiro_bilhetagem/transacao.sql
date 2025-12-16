-- depends_on: `rj-smtr`.`cadastro`.`operadoras_contato`
-- depends_on: `rj-smtr`.`cadastro`.`servico_operadora`
-- depends_on: `rj-smtr`.`br_rj_riodejaneiro_bilhetagem`.`transacao_riocard`








    
        

        

        

    


with
    transacao as (
        select *
        from `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`transacao`
         where 
  DATE(data) BETWEEN DATE("2022-01-01T00:00:00") AND DATE("2022-01-01T01:00:00")
  AND timestamp_captura BETWEEN DATETIME("2022-01-01T00:00:00") AND DATETIME("2022-01-01T01:00:00")
 
    ),
    tipo_transacao as (
        select chave as id_tipo_transacao, valor as tipo_transacao,
        from `rj-smtr.br_rj_riodejaneiro_bilhetagem.dicionario`
        where id_tabela = "transacao" and coluna = "id_tipo_transacao"
    ),
    gratuidade as (
        select
            cast(id_cliente as string) as id_cliente,
            tipo_gratuidade,
            data_inicio_validade,
            data_fim_validade
        from `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`gratuidade_aux`
    -- `rj-smtr.br_rj_riodejaneiro_bilhetagem_staging.gratuidade_aux`
    -- TODO: FILTRAR PARTIÇÕES DE FORMA EFICIENTE
    ),
    tipo_pagamento as (
        select chave as id_tipo_pagamento, valor as tipo_pagamento
        from `rj-smtr.br_rj_riodejaneiro_bilhetagem.dicionario`
        where id_tabela = "transacao" and coluna = "id_tipo_pagamento"
    ),
    integracao as (
        select id_transacao, valor_rateio, datetime_processamento_integracao
        from `rj-smtr`.`br_rj_riodejaneiro_bilhetagem`.`integracao`
        -- `rj-smtr.br_rj_riodejaneiro_bilhetagem.integracao`
        
            where
                 data = "2000-01-01"
                
        
        qualify
            row_number() over (
                partition by id_transacao
                order by datetime_processamento_integracao desc
            )
            = 1
    ),
    transacao_ordem as (
        select *
        from `rj-smtr`.`bilhetagem_staging`.`aux_transacao_id_ordem_pagamento`
        
            where
                 data = "2000-01-01"
                
        
    ),
    new_data as (
        select
            extract(date from data_transacao) as data,
            extract(hour from data_transacao) as hora,
            data_transacao as datetime_transacao,
            data_processamento as datetime_processamento,
            t.timestamp_captura as datetime_captura,
            m.modo,
            dc.id_consorcio,
            dc.consorcio,
            do.id_operadora,
            do.operadora,
            t.cd_linha as id_servico_jae,
            -- s.servico,
            l.nr_linha as servico_jae,
            l.nm_linha as descricao_servico_jae,
            sentido,
            case
                when m.modo = "VLT"
                then substring(t.veiculo_id, 1, 3)
                when m.modo = "BRT"
                then null
                else t.veiculo_id
            end as id_veiculo,
            t.numero_serie_validador as id_validador,
            coalesce(t.id_cliente, t.pan_hash) as id_cliente,
            id as id_transacao,
            tp.tipo_pagamento,
            tt.tipo_transacao,
            g.tipo_gratuidade,
            tipo_integracao as id_tipo_integracao,
            null as id_integracao,
            latitude_trx as latitude,
            longitude_trx as longitude,
            st_geogpoint(longitude_trx, latitude_trx) as geo_point_transacao,
            null as stop_id,
            null as stop_lat,
            null as stop_lon,
            valor_transacao
        from transacao as t
        left join
            `rj-smtr`.`cadastro`.`modos` m
            -- `rj-smtr.cadastro.modos` m
            on t.id_tipo_modal = m.id_modo
            and m.fonte = "jae"
        left join
            `rj-smtr`.`cadastro`.`operadoras` do
            -- `rj-smtr.cadastro.operadoras` do
            on t.cd_operadora = do.id_operadora_jae
        left join
            `rj-smtr`.`cadastro`.`consorcios` dc
            -- `rj-smtr.cadastro.consorcios` dc
            on t.cd_consorcio = dc.id_consorcio_jae
        left join `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`linha` l on t.cd_linha = l.cd_linha
        -- LEFT JOIN
        -- `rj-smtr`.`cadastro`.`servicos` AS s
        -- ON
        -- t.cd_linha = s.id_servico_jae
        left join tipo_transacao tt on tt.id_tipo_transacao = t.tipo_transacao
        left join tipo_pagamento tp on t.id_tipo_midia = tp.id_tipo_pagamento
        left join
            gratuidade g
            on t.tipo_transacao = "21"
            and t.id_cliente = g.id_cliente
            and t.data_transacao >= g.data_inicio_validade
            and (t.data_transacao < g.data_fim_validade or g.data_fim_validade is null)
        left join
            `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`linha_sem_ressarcimento` lsr
            on t.cd_linha = lsr.id_linha
        where lsr.id_linha is null and date(data_transacao) >= "2023-07-17"
    ),
    
        transacao_atual as (
            select *
            from `rj-smtr`.`br_rj_riodejaneiro_bilhetagem`.`transacao`
            where
                 data = "2000-01-01"
                
        ),
    
    complete_partitions as (
        select
            data,
            hora,
            datetime_transacao,
            datetime_processamento,
            datetime_captura,
            modo,
            id_consorcio,
            consorcio,
            id_operadora,
            operadora,
            id_servico_jae,
            servico_jae,
            descricao_servico_jae,
            sentido,
            id_veiculo,
            id_validador,
            id_cliente,
            id_transacao,
            tipo_pagamento,
            tipo_transacao,
            tipo_gratuidade,
            id_tipo_integracao,
            id_integracao,
            latitude,
            longitude,
            geo_point_transacao,
            stop_id,
            stop_lat,
            stop_lon,
            valor_transacao,
            0 as priority
        from new_data

        
            union all

            select
                data,
                hora,
                datetime_transacao,
                datetime_processamento,
                datetime_captura,
                modo,
                id_consorcio,
                consorcio,
                id_operadora,
                operadora,
                id_servico_jae,
                servico_jae,
                descricao_servico_jae,
                sentido,
                id_veiculo,
                id_validador,
                id_cliente,
                id_transacao,
                tipo_pagamento,
                tipo_transacao,
                tipo_gratuidade,
                id_tipo_integracao,
                id_integracao,
                latitude,
                longitude,
                geo_point_transacao,
                stop_id,
                stop_lat,
                stop_lon,
                valor_transacao,
                1 as priority
            from transacao_atual
        
    ),
    transacao_deduplicada as (
        select * except (rn)
        from
            (
                select
                    *,
                    row_number() over (
                        partition by id_transacao
                        order by datetime_captura desc, priority
                    ) as rn
                from complete_partitions
            )
        where rn = 1
    ),
    transacao_final as (
        select
            t.data,
            t.hora,
            t.datetime_transacao,
            t.datetime_processamento,
            t.datetime_captura,
            t.modo,
            t.id_consorcio,
            t.consorcio,
            t.id_operadora,
            t.operadora,
            t.id_servico_jae,
            t.servico_jae,
            t.descricao_servico_jae,
            t.sentido,
            t.id_veiculo,
            t.id_validador,
            t.id_cliente,
            sha256(t.id_cliente) as hash_cliente,
            t.id_transacao,
            t.tipo_pagamento,
            t.tipo_transacao,
            case
                when t.tipo_transacao = "Integração" or i.id_transacao is not null
                then "Integração"
                when t.tipo_transacao in ("Débito", "Botoeira")
                then "Integral"
                else t.tipo_transacao
            end as tipo_transacao_smtr,
            t.tipo_gratuidade,
            t.id_tipo_integracao,
            t.id_integracao,
            t.latitude,
            t.longitude,
            t.geo_point_transacao,
            t.stop_id,
            t.stop_lat,
            t.stop_lon,
            t.valor_transacao,
            case
                when
                    i.id_transacao is not null
                    or o.id_transacao is not null
                    or date(t.datetime_processamento)
                    < (select max(data_ordem) from `rj-smtr`.`br_rj_riodejaneiro_bilhetagem`.`ordem_pagamento_dia`)
                then coalesce(i.valor_rateio, t.valor_transacao) * 0.96
            end as valor_pagamento,
            o.data_ordem,
            o.id_ordem_pagamento_servico_operador_dia,
            o.id_ordem_pagamento_consorcio_operador_dia,
            o.id_ordem_pagamento_consorcio_dia,
            o.id_ordem_pagamento
        from transacao_deduplicada t
        left join integracao i using (id_transacao)
        left join transacao_ordem o using (id_transacao)
    )
    
select
    f.*,
    '' as versao,
    
        case
            when
                a.id_transacao is null
                or sha256(
                    concat(
                        
                            ifnull(cast(f.data as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(f.hora as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(f.datetime_transacao as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(f.datetime_processamento as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(f.datetime_captura as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(f.modo as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(f.id_consorcio as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(f.consorcio as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(f.id_operadora as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(f.operadora as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(f.id_servico_jae as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(f.servico_jae as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(f.descricao_servico_jae as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(f.sentido as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(f.id_veiculo as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(f.id_validador as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(f.id_cliente as string), 'n/a')
                            

                            , 

                        
                            
                                ifnull(to_base64(f.hash_cliente), 'n/a')
                            

                            , 

                        
                            ifnull(cast(f.id_transacao as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(f.tipo_pagamento as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(f.tipo_transacao as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(f.tipo_transacao_smtr as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(f.tipo_gratuidade as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(f.id_tipo_integracao as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(f.id_integracao as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(f.latitude as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(f.longitude as string), 'n/a')
                            

                            , 

                        
                            
                                ifnull(st_astext(f.geo_point_transacao), 'n/a')
                            

                            , 

                        
                            ifnull(cast(f.stop_id as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(f.stop_lat as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(f.stop_lon as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(f.valor_transacao as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(f.valor_pagamento as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(f.data_ordem as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(f.id_ordem_pagamento_servico_operador_dia as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(f.id_ordem_pagamento_consorcio_operador_dia as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(f.id_ordem_pagamento_consorcio_dia as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(f.id_ordem_pagamento as string), 'n/a')
                            

                            

                        
                    )
                ) != sha256(
                    concat(
                        
                            ifnull(cast(a.data as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(a.hora as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(a.datetime_transacao as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(a.datetime_processamento as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(a.datetime_captura as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(a.modo as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(a.id_consorcio as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(a.consorcio as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(a.id_operadora as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(a.operadora as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(a.id_servico_jae as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(a.servico_jae as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(a.descricao_servico_jae as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(a.sentido as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(a.id_veiculo as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(a.id_validador as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(a.id_cliente as string), 'n/a')
                            

                            , 

                        
                            
                                ifnull(to_base64(f.hash_cliente), 'n/a')
                            

                            , 

                        
                            ifnull(cast(a.id_transacao as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(a.tipo_pagamento as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(a.tipo_transacao as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(a.tipo_transacao_smtr as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(a.tipo_gratuidade as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(a.id_tipo_integracao as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(a.id_integracao as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(a.latitude as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(a.longitude as string), 'n/a')
                            

                            , 

                        
                            
                                ifnull(st_astext(a.geo_point_transacao), 'n/a')
                            

                            , 

                        
                            ifnull(cast(a.stop_id as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(a.stop_lat as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(a.stop_lon as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(a.valor_transacao as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(a.valor_pagamento as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(a.data_ordem as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(a.id_ordem_pagamento_servico_operador_dia as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(a.id_ordem_pagamento_consorcio_operador_dia as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(a.id_ordem_pagamento_consorcio_dia as string), 'n/a')
                            

                            , 

                        
                            ifnull(cast(a.id_ordem_pagamento as string), 'n/a')
                            

                            

                        
                    )
                )
            then current_datetime("America/Sao_Paulo")
            else a.datetime_ultima_atualizacao
        end
     as datetime_ultima_atualizacao
from transacao_final f
 left join transacao_atual a using (id_transacao) 