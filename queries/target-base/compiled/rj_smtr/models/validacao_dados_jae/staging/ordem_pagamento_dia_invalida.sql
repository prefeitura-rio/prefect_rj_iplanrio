

WITH ordem_pagamento_consorcio_dia AS (
  SELECT
    data_ordem,
    id_ordem_pagamento,
    SUM(quantidade_total_transacao) AS quantidade_total_transacao,
    SUM(valor_total_transacao_liquido) AS valor_total_transacao_liquido
  FROM
    `rj-smtr`.`br_rj_riodejaneiro_bilhetagem`.`ordem_pagamento_consorcio_dia`
  
    WHERE
      data_ordem = DATE("2022-01-01T01:00:00")
  
  GROUP BY
    1,
    2
),
ordem_pagamento_dia AS (
  SELECT
    data_ordem,
    id_ordem_pagamento,
    quantidade_total_transacao,
    valor_total_transacao_liquido
  FROM
    `rj-smtr`.`br_rj_riodejaneiro_bilhetagem`.`ordem_pagamento_dia`
  
    WHERE
      data_ordem = DATE("2022-01-01T01:00:00")
  
),
indicadores AS (
SELECT
  d.data_ordem,
  d.id_ordem_pagamento,
  d.quantidade_total_transacao,
  cd.quantidade_total_transacao AS quantidade_total_transacao_agregacao,
  d.valor_total_transacao_liquido,
  cd.valor_total_transacao_liquido AS valor_total_transacao_liquido_agregacao,
  ROUND(cd.valor_total_transacao_liquido, 2) != ROUND(d.valor_total_transacao_liquido, 2) OR cd.quantidade_total_transacao != d.quantidade_total_transacao AS indicador_agregacao_invalida
FROM
  ordem_pagamento_dia d
LEFT JOIN
  ordem_pagamento_consorcio_dia cd
USING(data_ordem, id_ordem_pagamento)
)
SELECT
  *,
  '' AS versao
FROM
  indicadores
WHERE
  indicador_agregacao_invalida = TRUE