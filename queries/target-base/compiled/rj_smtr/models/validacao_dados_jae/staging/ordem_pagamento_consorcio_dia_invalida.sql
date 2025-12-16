-- depends_on: `rj-smtr`.`validacao_dados_jae_staging`.`ordem_pagamento_consorcio_operador_dia_invalida`


WITH ordem_pagamento_consorcio_operador_dia AS (
  SELECT
    data_ordem,
    id_consorcio,
    id_ordem_pagamento,
    SUM(quantidade_total_transacao) AS quantidade_total_transacao,
    SUM(valor_total_transacao_liquido_ordem) AS valor_total_transacao_liquido,
  FROM
    `rj-smtr`.`br_rj_riodejaneiro_bilhetagem`.`ordem_pagamento_consorcio_operador_dia`
  
    WHERE
      data_ordem = DATE("2022-01-01T01:00:00")
  
  GROUP BY
    1,
    2,
    3
),
ordem_pagamento_consorcio_dia AS (
  SELECT
    data_ordem,
    id_consorcio,
    id_ordem_pagamento,
    quantidade_total_transacao,
    valor_total_transacao_liquido
  FROM
    `rj-smtr`.`br_rj_riodejaneiro_bilhetagem`.`ordem_pagamento_consorcio_dia`
  
    WHERE
      data_ordem = DATE("2022-01-01T01:00:00")
  
),
indicadores AS (
  SELECT
    cd.data_ordem,
    cd.id_consorcio,
    cd.id_ordem_pagamento,
    cd.quantidade_total_transacao,
    cod.quantidade_total_transacao AS quantidade_total_transacao_agregacao,
    cd.valor_total_transacao_liquido,
    cod.valor_total_transacao_liquido AS valor_total_transacao_liquido_agregacao,
    ROUND(cd.valor_total_transacao_liquido, 2) != ROUND(cod.valor_total_transacao_liquido, 2) OR cd.quantidade_total_transacao != cod.quantidade_total_transacao AS indicador_agregacao_invalida
  FROM
    ordem_pagamento_consorcio_dia cd
  LEFT JOIN
    ordem_pagamento_consorcio_operador_dia cod
  USING(
    data_ordem,
    id_consorcio,
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