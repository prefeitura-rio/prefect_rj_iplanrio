

-- depends_on: `rj-smtr`.`br_rj_riodejaneiro_bilhetagem`.`ordem_pagamento_consorcio_dia`
SELECT
    o.data_ordem,
    o.data_pagamento,
    o.id_ordem_pagamento AS id_ordem_pagamento,
    o.qtd_debito AS quantidade_transacao_debito,
    o.valor_debito,
    o.qtd_vendaabordo AS quantidade_transacao_especie,
    o.valor_vendaabordo AS valor_especie,
    o.qtd_gratuidade AS quantidade_transacao_gratuidade,
    o.valor_gratuidade,
    o.qtd_integracao AS quantidade_transacao_integracao,
    o.valor_integracao,
    o.qtd_rateio_credito AS quantidade_transacao_rateio_credito,
    o.valor_rateio_credito AS valor_rateio_credito,
    o.qtd_rateio_debito AS quantidade_transacao_rateio_debito,
    o.valor_rateio_debito AS valor_rateio_debito,
    (
      o.qtd_debito
      + o.qtd_vendaabordo
      + o.qtd_gratuidade
      + o.qtd_integracao
    ) AS quantidade_total_transacao,
    o.valor_bruto AS valor_total_transacao_bruto,
    o.valor_taxa AS valor_desconto_taxa,
    o.valor_liquido AS valor_total_transacao_liquido,
    '' AS versao
FROM
    `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`ordem_pagamento` o

    WHERE
        DATE(o.data) BETWEEN DATE("2022-01-01T00:00:00") AND DATE("2022-01-01T01:00:00")
