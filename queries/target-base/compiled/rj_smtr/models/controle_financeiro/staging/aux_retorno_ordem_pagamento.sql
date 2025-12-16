

SELECT DISTINCT
  dataOrdem AS data_ordem,
  DATE(dataVencimento) AS data_pagamento,
  idConsorcio AS id_consorcio,
  idOperadora AS id_operadora,
  CONCAT(dataOrdem, idConsorcio, idOperadora) AS unique_id,
  valorRealEfetivado AS valor_pago
FROM
  `rj-smtr`.`controle_financeiro_staging`.`arquivo_retorno`
WHERE
  isPago = TRUE

  AND DATE(data) BETWEEN DATE("2022-01-01T00:00:00") AND DATE("2022-01-01T01:00:00")
