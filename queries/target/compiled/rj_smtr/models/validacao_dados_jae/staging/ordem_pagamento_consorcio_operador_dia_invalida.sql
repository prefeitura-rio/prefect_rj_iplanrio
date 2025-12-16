-- depends_on: `rj-smtr`.`validacao_dados_jae_staging`.`ordem_pagamento_servico_operador_dia_invalida`


WITH ordem_pagamento_servico_operador_dia AS (
  SELECT
    data_ordem,
    id_consorcio,
    id_operadora,
    id_ordem_pagamento,
    SUM(quantidade_total_transacao) AS quantidade_total_transacao,
    SUM(valor_total_transacao_liquido) AS valor_total_transacao_liquido,
  FROM
    `rj-smtr`.`br_rj_riodejaneiro_bilhetagem`.`ordem_pagamento_servico_operador_dia`
  
    WHERE
      data_ordem = DATE("2022-01-01T01:00:00")
  
  GROUP BY
    1,
    2,
    3,
    4
),
ordem_pagamento_consorcio_operador_dia AS (
  SELECT
    data_ordem,
    id_consorcio,
    id_operadora,
    id_ordem_pagamento,
    quantidade_total_transacao,
    valor_total_transacao_liquido_ordem AS valor_total_transacao_liquido
  FROM
    `rj-smtr`.`br_rj_riodejaneiro_bilhetagem`.`ordem_pagamento_consorcio_operador_dia`
  
    WHERE
      data_ordem = DATE("2022-01-01T01:00:00")
  
),
indicadores AS (
  SELECT
    cod.data_ordem,
    cod.id_consorcio,
    cod.id_operadora,
    cod.id_ordem_pagamento,
    cod.quantidade_total_transacao,
    sod.quantidade_total_transacao AS quantidade_total_transacao_agregacao,
    cod.valor_total_transacao_liquido,
    sod.valor_total_transacao_liquido AS valor_total_transacao_liquido_agregacao,
    ROUND(cod.valor_total_transacao_liquido, 2) != ROUND(sod.valor_total_transacao_liquido, 2) OR cod.quantidade_total_transacao != sod.quantidade_total_transacao AS indicador_agregacao_invalida
  FROM
    ordem_pagamento_consorcio_operador_dia cod
  LEFT JOIN
    ordem_pagamento_servico_operador_dia sod
  USING(
    data_ordem,
    id_consorcio,
    id_operadora,
    id_ordem_pagamento
  )
)
SELECT
  *,
  '' AS versao
FROM
  indicadores
WHERE
  indicador_agregacao_invalida = TRUE