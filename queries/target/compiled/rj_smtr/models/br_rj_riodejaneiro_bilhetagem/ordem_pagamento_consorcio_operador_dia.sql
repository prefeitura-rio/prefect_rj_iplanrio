
-- depends_on: `rj-smtr`.`br_rj_riodejaneiro_bilhetagem`.`ordem_pagamento_servico_operador_dia`




    
        -- Verifica as ordens de pagamento capturadas
        

        

        

        
    



with
    pagamento as (
        select data_pagamento, data_ordem, id_consorcio, id_operadora, valor_pago
        from `rj-smtr`.`controle_financeiro_staging`.`aux_retorno_ordem_pagamento`
        -- `rj-smtr.controle_financeiro_staging.aux_retorno_ordem_pagamento`
        
            where
                 data_ordem = '2000-01-01'
                
                

        
    ),
    ordem_pagamento as (
        select
            o.data_ordem,
            o.id_ordem_pagamento_consorcio_operadora
            as id_ordem_pagamento_consorcio_operador_dia,
            dc.id_consorcio,
            dc.consorcio,
            do.id_operadora,
            do.operadora,
            op.id_ordem_pagamento as id_ordem_pagamento,
            o.qtd_debito as quantidade_transacao_debito,
            o.valor_debito,
            o.qtd_vendaabordo as quantidade_transacao_especie,
            o.valor_vendaabordo as valor_especie,
            o.qtd_gratuidade as quantidade_transacao_gratuidade,
            o.valor_gratuidade,
            o.qtd_integracao as quantidade_transacao_integracao,
            o.valor_integracao,
            o.qtd_rateio_credito as quantidade_transacao_rateio_credito,
            o.valor_rateio_credito as valor_rateio_credito,
            o.qtd_rateio_debito as quantidade_transacao_rateio_debito,
            o.valor_rateio_debito as valor_rateio_debito,
            (
                o.qtd_debito + o.qtd_vendaabordo + o.qtd_gratuidade + o.qtd_integracao
            ) as quantidade_total_transacao,
            o.valor_bruto as valor_total_transacao_bruto,
            o.valor_taxa as valor_desconto_taxa,
            o.valor_liquido as valor_total_transacao_liquido_ordem,
            o.timestamp_captura as datetime_captura,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
        from `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`ordem_pagamento_consorcio_operadora` o
        -- `rj-smtr.br_rj_riodejaneiro_bilhetagem_staging.ordem_pagamento_consorcio_operadora` o
        join
            `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`ordem_pagamento` op
            
            on o.data_ordem = op.data_ordem
        left join `rj-smtr`.`cadastro`.`operadoras` do on o.id_operadora = do.id_operadora_jae
        
        left join `rj-smtr`.`cadastro`.`consorcios` dc on o.id_consorcio = dc.id_consorcio_jae
        
        
            where
                date(o.data) between date("2022-01-01T00:00:00") and date(
                    "2022-01-01T01:00:00"
                )
                and o.timestamp_captura > datetime("2022-01-01T00:00:00")
                and o.timestamp_captura <= datetime("2022-01-01T01:00:00")
        
    ),
    ordem_pagamento_completa as (
        select *, 0 as priority
        from ordem_pagamento

        
    ),
    ordem_valor_pagamento as (
        select
            data_ordem,
            id_ordem_pagamento_consorcio_operador_dia,
            id_consorcio,
            o.consorcio,
            id_operadora,
            o.operadora,
            o.id_ordem_pagamento,
            o.quantidade_transacao_debito,
            o.valor_debito,
            o.quantidade_transacao_especie,
            o.valor_especie,
            o.quantidade_transacao_gratuidade,
            o.valor_gratuidade,
            o.quantidade_transacao_integracao,
            o.valor_integracao,
            o.quantidade_transacao_rateio_credito,
            o.valor_rateio_credito,
            o.quantidade_transacao_rateio_debito,
            o.valor_rateio_debito,
            o.quantidade_total_transacao,
            o.valor_total_transacao_bruto,
            o.valor_desconto_taxa,
            o.valor_total_transacao_liquido_ordem,
            p.data_pagamento,
            p.valor_pago,
            o.datetime_captura,
            o.datetime_ultima_atualizacao,
            row_number() over (
                partition by data_ordem, id_consorcio, id_operadora order by priority
            ) as rn
        from ordem_pagamento_completa o
        left join pagamento p using (data_ordem, id_consorcio, id_operadora)
    )
select
    data_ordem,
    id_ordem_pagamento_consorcio_operador_dia,
    id_consorcio,
    consorcio,
    id_operadora,
    operadora,
    id_ordem_pagamento,
    quantidade_transacao_debito,
    valor_debito,
    quantidade_transacao_especie,
    valor_especie,
    quantidade_transacao_gratuidade,
    valor_gratuidade,
    quantidade_transacao_integracao,
    valor_integracao,
    quantidade_transacao_rateio_credito,
    valor_rateio_credito,
    quantidade_transacao_rateio_debito,
    valor_rateio_debito,
    quantidade_total_transacao,
    valor_total_transacao_bruto,
    valor_desconto_taxa,
    valor_total_transacao_liquido_ordem,
    case
        when data_ordem = '2024-06-07' and id_consorcio = '2' and id_operadora = '8'
        then valor_total_transacao_liquido_ordem - 1403.4532  -- Corrigir valor pago incorretamente ao VLT na ordem do dia 2024-05-31
        else valor_total_transacao_liquido_ordem
    end as valor_total_transacao_liquido,
    data_pagamento,
    valor_pago,
    datetime_captura,
    '' as versao,
    datetime_ultima_atualizacao
from ordem_valor_pagamento
where rn = 1