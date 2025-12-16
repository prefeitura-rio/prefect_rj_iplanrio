-- depends_on: `rj-smtr`.`planejamento`.`matriz_integracao`







    
        

        

    


with
    integracao_transacao_deduplicada as (
        select * except (rn)
        from
            (
                select
                    *,
                    row_number() over (
                        partition by id order by timestamp_captura desc
                    ) as rn
                from `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`integracao_transacao`
                
                where 
    date(data) between date("2022-01-01T00:00:00") and date(
                            "2022-01-01T01:00:00"
    )
    and timestamp_captura
    between datetime("2022-01-01T00:00:00") and datetime(
        "2022-01-01T01:00:00"
                        )

            )
        where rn = 1
    ),
    integracao_melt as (
        select
            extract(date from im.data_transacao) as data,
            extract(hour from im.data_transacao) as hora,
            i.data_inclusao as datetime_inclusao,
            i.data_processamento as datetime_processamento_integracao,
            i.timestamp_captura as datetime_captura,
            i.id as id_integracao,
            im.sequencia_integracao,
            im.data_transacao as datetime_transacao,
            im.id_tipo_modal,
            im.id_consorcio,
            im.id_operadora,
            im.id_linha,
            im.id_transacao,
            im.sentido,
            im.perc_rateio,
            im.valor_rateio_compensacao,
            im.valor_rateio,
            im.valor_transacao,
            i.valor_transacao_total,
            i.tx_adicional as texto_adicional,
            im.id_ordem_rateio
        from
            integracao_transacao_deduplicada i,
            -- Transforma colunas com os dados de cada transação da integração em
            -- linhas diferentes
            unnest(
                [
                    
                        struct(
                            
                                
                                    data_transacao_t0 as data_transacao,
                                
                            
                                
                            
                                
                                    id_consorcio_t0 as id_consorcio,
                                
                            
                                
                            
                                
                                    id_linha_t0 as id_linha,
                                
                            
                                
                            
                                
                                    id_operadora_t0 as id_operadora,
                                
                            
                                
                                    id_ordem_rateio_t0 as id_ordem_rateio,
                                
                            
                                
                            
                                
                            
                                
                                    id_tipo_modal_t0 as id_tipo_modal,
                                
                            
                                
                                    id_transacao_t0 as id_transacao,
                                
                            
                                
                            
                                
                            
                                
                            
                                
                                    perc_rateio_t0 as perc_rateio,
                                
                            
                                
                            
                                
                                    sentido_t0 as sentido,
                                
                            
                                
                                    valor_rateio_compensacao_t0 as valor_rateio_compensacao,
                                
                            
                                
                                    valor_rateio_t0 as valor_rateio,
                                
                            
                                
                                    valor_tarifa_t0 as valor_tarifa,
                                
                            
                                
                                    valor_transacao_t0 as valor_transacao,
                                
                            
                                
                            
                            1 as sequencia_integracao
                        )
                        ,
                    
                        struct(
                            
                                
                                    data_transacao_t1 as data_transacao,
                                
                            
                                
                            
                                
                                    id_consorcio_t1 as id_consorcio,
                                
                            
                                
                            
                                
                                    id_linha_t1 as id_linha,
                                
                            
                                
                            
                                
                                    id_operadora_t1 as id_operadora,
                                
                            
                                
                                    id_ordem_rateio_t1 as id_ordem_rateio,
                                
                            
                                
                            
                                
                            
                                
                                    id_tipo_modal_t1 as id_tipo_modal,
                                
                            
                                
                                    id_transacao_t1 as id_transacao,
                                
                            
                                
                            
                                
                            
                                
                            
                                
                                    perc_rateio_t1 as perc_rateio,
                                
                            
                                
                            
                                
                                    sentido_t1 as sentido,
                                
                            
                                
                                    valor_rateio_compensacao_t1 as valor_rateio_compensacao,
                                
                            
                                
                                    valor_rateio_t1 as valor_rateio,
                                
                            
                                
                                    valor_tarifa_t1 as valor_tarifa,
                                
                            
                                
                                    valor_transacao_t1 as valor_transacao,
                                
                            
                                
                            
                            2 as sequencia_integracao
                        )
                        ,
                    
                        struct(
                            
                                
                                    data_transacao_t2 as data_transacao,
                                
                            
                                
                            
                                
                                    id_consorcio_t2 as id_consorcio,
                                
                            
                                
                            
                                
                                    id_linha_t2 as id_linha,
                                
                            
                                
                            
                                
                                    id_operadora_t2 as id_operadora,
                                
                            
                                
                                    id_ordem_rateio_t2 as id_ordem_rateio,
                                
                            
                                
                            
                                
                            
                                
                                    id_tipo_modal_t2 as id_tipo_modal,
                                
                            
                                
                                    id_transacao_t2 as id_transacao,
                                
                            
                                
                            
                                
                            
                                
                            
                                
                                    perc_rateio_t2 as perc_rateio,
                                
                            
                                
                            
                                
                                    sentido_t2 as sentido,
                                
                            
                                
                                    valor_rateio_compensacao_t2 as valor_rateio_compensacao,
                                
                            
                                
                                    valor_rateio_t2 as valor_rateio,
                                
                            
                                
                                    valor_tarifa_t2 as valor_tarifa,
                                
                            
                                
                                    valor_transacao_t2 as valor_transacao,
                                
                            
                                
                            
                            3 as sequencia_integracao
                        )
                        ,
                    
                        struct(
                            
                                
                                    data_transacao_t3 as data_transacao,
                                
                            
                                
                            
                                
                                    id_consorcio_t3 as id_consorcio,
                                
                            
                                
                            
                                
                                    id_linha_t3 as id_linha,
                                
                            
                                
                            
                                
                                    id_operadora_t3 as id_operadora,
                                
                            
                                
                                    id_ordem_rateio_t3 as id_ordem_rateio,
                                
                            
                                
                            
                                
                            
                                
                                    id_tipo_modal_t3 as id_tipo_modal,
                                
                            
                                
                                    id_transacao_t3 as id_transacao,
                                
                            
                                
                            
                                
                            
                                
                            
                                
                                    perc_rateio_t3 as perc_rateio,
                                
                            
                                
                            
                                
                                    sentido_t3 as sentido,
                                
                            
                                
                                    valor_rateio_compensacao_t3 as valor_rateio_compensacao,
                                
                            
                                
                                    valor_rateio_t3 as valor_rateio,
                                
                            
                                
                                    valor_tarifa_t3 as valor_tarifa,
                                
                            
                                
                                    valor_transacao_t3 as valor_transacao,
                                
                            
                                
                            
                            4 as sequencia_integracao
                        )
                        ,
                    
                        struct(
                            
                                
                                    data_transacao_t4 as data_transacao,
                                
                            
                                
                            
                                
                                    id_consorcio_t4 as id_consorcio,
                                
                            
                                
                            
                                
                                    id_linha_t4 as id_linha,
                                
                            
                                
                            
                                
                                    id_operadora_t4 as id_operadora,
                                
                            
                                
                                    id_ordem_rateio_t4 as id_ordem_rateio,
                                
                            
                                
                            
                                
                            
                                
                                    id_tipo_modal_t4 as id_tipo_modal,
                                
                            
                                
                                    id_transacao_t4 as id_transacao,
                                
                            
                                
                            
                                
                            
                                
                            
                                
                                    perc_rateio_t4 as perc_rateio,
                                
                            
                                
                            
                                
                                    sentido_t4 as sentido,
                                
                            
                                
                                    valor_rateio_compensacao_t4 as valor_rateio_compensacao,
                                
                            
                                
                                    valor_rateio_t4 as valor_rateio,
                                
                            
                                
                                    valor_tarifa_t4 as valor_tarifa,
                                
                            
                                
                                    valor_transacao_t4 as valor_transacao,
                                
                            
                                
                            
                            5 as sequencia_integracao
                        )
                        
                    
                ]
            ) as im
    ),
    integracao_new as (
        select
            i.data,
            i.hora,
            i.datetime_processamento_integracao,
            i.datetime_captura,
            i.datetime_transacao,
            timestamp_diff(
                i.datetime_transacao,
                lag(i.datetime_transacao) over (
                    partition by i.id_integracao order by sequencia_integracao
                ),
                minute
            ) as intervalo_integracao,
            i.id_integracao,
            i.sequencia_integracao,
            m.modo,
            dc.id_consorcio,
            dc.consorcio,
            do.id_operadora,
            do.operadora,
            i.id_linha as id_servico_jae,
            l.nr_linha as servico_jae,
            l.nm_linha as descricao_servico_jae,
            i.id_transacao,
            i.sentido,
            i.perc_rateio as percentual_rateio,
            i.valor_rateio_compensacao,
            i.valor_rateio,
            i.valor_transacao,
            i.valor_transacao_total,
            i.texto_adicional,
            i.id_ordem_rateio,
            o.data_ordem,
            o.id_ordem_pagamento,
            o.id_ordem_pagamento_consorcio as id_ordem_pagamento_consorcio_dia,
            o.id_ordem_pagamento_consorcio_operadora
            as id_ordem_pagamento_consorcio_operador_dia,
            '' as versao
        from integracao_melt i
        left join
            `rj-smtr`.`cadastro`.`modos` m
            on i.id_tipo_modal = m.id_modo
            and m.fonte = "jae"
        left join `rj-smtr`.`cadastro`.`operadoras` do on i.id_operadora = do.id_operadora_jae
        
        left join `rj-smtr`.`cadastro`.`consorcios` dc on i.id_consorcio = dc.id_consorcio_jae
        
        left join
            `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`linha` l
            
            on i.id_linha = l.cd_linha
        left join `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`ordem_rateio` o using (id_ordem_rateio)
        where i.id_transacao is not null
    ),
    complete_partitions as (
        select *, 0 as priority
        from integracao_new

        
            union all

            select *, 1 as priority
            from `rj-smtr`.`br_rj_riodejaneiro_bilhetagem`.`integracao`
            where
                 data = "2000-01-01"
                
        
    ),
    integracoes_teste_invalidas as (
        select distinct i.id_integracao
        from complete_partitions i
        left join
            `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`linha_sem_ressarcimento` l
            
            on i.id_servico_jae = l.id_linha
        where l.id_linha is not null or i.data < "2023-07-17"
    )
select * except (priority)
from complete_partitions
where id_integracao not in (select id_integracao from integracoes_teste_invalidas)
qualify
    row_number() over (
        partition by id_integracao, id_transacao
        order by datetime_processamento_integracao desc, priority
    )
    = 1