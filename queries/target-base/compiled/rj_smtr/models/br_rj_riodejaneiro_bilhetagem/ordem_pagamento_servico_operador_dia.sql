

WITH ordem_pagamento AS (
    SELECT
        r.data_ordem,
        dc.id_consorcio,
        dc.consorcio,
        do.id_operadora,
        do.operadora,
        r.id_linha AS id_servico_jae,
        -- s.servico,
        l.nr_linha AS servico_jae,
        l.nm_linha AS descricao_servico_jae,
        r.id_ordem_pagamento AS id_ordem_pagamento,
        r.id_ordem_ressarcimento AS id_ordem_ressarcimento,
        r.qtd_debito AS quantidade_transacao_debito,
        r.valor_debito,
        r.qtd_vendaabordo AS quantidade_transacao_especie,
        r.valor_vendaabordo AS valor_especie,
        r.qtd_gratuidade AS quantidade_transacao_gratuidade,
        r.valor_gratuidade,
        r.qtd_integracao AS quantidade_transacao_integracao,
        r.valor_integracao,
        COALESCE(rat.qtd_rateio_compensacao_credito_total, r.qtd_rateio_credito) AS quantidade_transacao_rateio_credito,
        COALESCE(rat.valor_rateio_compensacao_credito_total, r.valor_rateio_credito) AS valor_rateio_credito,
        COALESCE(rat.qtd_rateio_compensacao_debito_total, r.qtd_rateio_debito) AS quantidade_transacao_rateio_debito,
        COALESCE(rat.valor_rateio_compensacao_debito_total, r.valor_rateio_debito) AS valor_rateio_debito,
        (
            r.qtd_debito
            + r.qtd_vendaabordo
            + r.qtd_gratuidade
            + r.qtd_integracao
        ) AS quantidade_total_transacao,
        r.valor_bruto AS valor_total_transacao_bruto,
        r.valor_taxa AS valor_desconto_taxa,
        r.valor_liquido AS valor_total_transacao_liquido
    FROM
        `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`ordem_ressarcimento` r
    LEFT JOIN
        `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`ordem_rateio` rat
    USING(data_ordem, id_consorcio, id_operadora, id_linha)
    LEFT JOIN
        `rj-smtr`.`cadastro`.`operadoras` AS do
    ON
        r.id_operadora = do.id_operadora_jae
    LEFT JOIN
        `rj-smtr`.`cadastro`.`consorcios` AS dc
    ON
        r.id_consorcio = dc.id_consorcio_jae
    LEFT JOIN
        `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`linha` AS l
    ON
        r.id_linha = l.cd_linha
    -- LEFT JOIN
    --     `rj-smtr`.`cadastro`.`servicos` AS s
    -- ON
    --     r.id_linha = s.id_servico_jae
    
        WHERE
            DATE(r.data) BETWEEN DATE("2022-01-01T00:00:00") AND DATE("2022-01-01T01:00:00")
    
)
SELECT
    o.data_ordem,
    o.id_consorcio,
    o.consorcio,
    o.id_operadora,
    o.operadora,
    o.id_servico_jae,
    o.servico_jae,
    o.descricao_servico_jae,
    o.id_ordem_pagamento,
    o.id_ordem_ressarcimento,
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
    o.valor_total_transacao_bruto + o.valor_rateio_debito + o.valor_rateio_credito AS valor_total_transacao_bruto,
    o.valor_desconto_taxa,
    o.valor_total_transacao_liquido + o.valor_rateio_debito + o.valor_rateio_credito AS valor_total_transacao_liquido,
    '' AS versao
FROM
    ordem_pagamento o