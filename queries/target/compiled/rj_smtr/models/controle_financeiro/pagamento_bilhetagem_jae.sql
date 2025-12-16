


WITH arquivo_retorno AS (
  SELECT
    id,
    DATE(dataVencimento) AS data,
    dataVencimento AS datetime_pagamento,
    timestamp_captura AS datetime_captura,
    idConsorcio AS id_consorcio,
    idOperadora AS id_operadora,
    dataOrdem AS data_ordem,
    idOrdemPagamento AS id_ordem_pagamento,
    favorecido,
    valor AS valor_ordem,
    valorRealEfetivado AS valor_pago,
    isPago AS indicador_pagamento_realizado
  FROM
    `rj-smtr`.`controle_financeiro_staging`.`arquivo_retorno`
),
arquivo_retorno_deduplicado AS (
  SELECT
    * EXCEPT(rn)
  FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY id_consorcio, id_operadora, data_ordem ORDER BY indicador_pagamento_realizado DESC, datetime_captura DESC) AS rn
    FROM
      arquivo_retorno
  )
  WHERE
    rn = 1
)
SELECT
  a.data,
  a.datetime_pagamento,
  a.datetime_captura,
  a.id_consorcio,
  a.id_operadora,
  c.modo,
  CASE
    WHEN c.modo = "Van" THEN c.consorcio
    ELSE a.favorecido
  END AS favorecido,
  a.data_ordem,
  a.id_ordem_pagamento,
  a.valor_ordem,
  a.valor_pago,
  a.indicador_pagamento_realizado,
  '' AS versao
FROM
  arquivo_retorno_deduplicado a
LEFT JOIN
  `rj-smtr`.`cadastro`.`consorcios` c
USING(id_consorcio)