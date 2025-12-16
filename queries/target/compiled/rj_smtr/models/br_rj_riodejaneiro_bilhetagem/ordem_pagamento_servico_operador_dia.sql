

with
    ordem_pagamento as (
        select
            r.data_ordem,
            r.id_ordem_ressarcimento as id_ordem_pagamento_servico_operador_dia,
            dc.id_consorcio,
            dc.consorcio,
            do.id_operadora,
            do.operadora,
            r.id_linha as id_servico_jae,
            l.nr_linha as servico_jae,
            l.nm_linha as descricao_servico_jae,
            r.id_ordem_pagamento_consorcio_operadora
            as id_ordem_pagamento_consorcio_operador_dia,
            r.id_ordem_pagamento_consorcio as id_ordem_pagamento_consorcio_dia,
            r.id_ordem_pagamento as id_ordem_pagamento,
            rat.id_ordem_rateio as id_ordem_rateio,
            r.qtd_debito as quantidade_transacao_debito,
            r.valor_debito,
            r.qtd_vendaabordo as quantidade_transacao_especie,
            r.valor_vendaabordo as valor_especie,
            r.qtd_gratuidade as quantidade_transacao_gratuidade,
            r.valor_gratuidade,
            r.qtd_integracao as quantidade_transacao_integracao,
            r.valor_integracao,
            coalesce(
                rat.qtd_rateio_compensacao_credito_total, r.qtd_rateio_credito
            ) as quantidade_transacao_rateio_credito,
            coalesce(
                rat.valor_rateio_compensacao_credito_total, r.valor_rateio_credito
            ) as valor_rateio_credito,
            coalesce(
                rat.qtd_rateio_compensacao_debito_total, r.qtd_rateio_debito
            ) as quantidade_transacao_rateio_debito,
            coalesce(
                rat.valor_rateio_compensacao_debito_total, r.valor_rateio_debito
            ) as valor_rateio_debito,
            (
                r.qtd_debito + r.qtd_vendaabordo + r.qtd_gratuidade + r.qtd_integracao
            ) as quantidade_total_transacao,
            r.valor_bruto as valor_total_transacao_bruto,
            r.valor_taxa as valor_desconto_taxa,
            r.valor_liquido as valor_total_transacao_liquido
        from `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`ordem_ressarcimento` r
        left join
            `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`ordem_rateio` rat using (
                data_ordem, id_consorcio, id_operadora, id_linha
            )
        left join `rj-smtr`.`cadastro`.`operadoras` as do on r.id_operadora = do.id_operadora_jae
        left join `rj-smtr`.`cadastro`.`consorcios` as dc on r.id_consorcio = dc.id_consorcio_jae
        left join `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`linha` as l on r.id_linha = l.cd_linha
        
            where
                date(r.data) between date("2022-01-01T00:00:00") and date(
                    "2022-01-01T01:00:00"
                )
        
    )
select
    data_ordem,
    id_ordem_pagamento_servico_operador_dia,
    id_consorcio,
    consorcio,
    id_operadora,
    operadora,
    id_servico_jae,
    servico_jae,
    descricao_servico_jae,
    id_ordem_pagamento_consorcio_operador_dia,
    id_ordem_pagamento_consorcio_dia,
    id_ordem_pagamento,
    id_ordem_rateio,
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
    valor_total_transacao_bruto
    + valor_rateio_debito
    + valor_rateio_credito as valor_total_transacao_bruto,
    valor_desconto_taxa,
    valor_total_transacao_liquido
    + valor_rateio_debito
    + valor_rateio_credito as valor_total_transacao_liquido,
    '' as versao
from ordem_pagamento o