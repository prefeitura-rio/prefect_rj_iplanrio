

WITH servico_operador_dia_invalida AS (
  SELECT
    data_ordem,
    id_ordem_pagamento,
    MAX(indicador_captura_invalida) AS indicador_captura_invalida,
    MAX(indicador_servico_fora_vigencia) AS indicador_servico_fora_vigencia
  FROM
    `rj-smtr`.`validacao_dados_jae_staging`.`ordem_pagamento_servico_operador_dia_invalida`
  
    WHERE
      data_ordem = DATE("2022-01-01T01:00:00")
  
  GROUP BY
    1,
    2
),
consorcio_operador_dia_invalida AS (
  SELECT DISTINCT
    data_ordem,
    id_ordem_pagamento
  FROM
    `rj-smtr`.`validacao_dados_jae_staging`.`ordem_pagamento_consorcio_operador_dia_invalida`
  
    WHERE
      data_ordem = DATE("2022-01-01T01:00:00")
  
),
consorcio_dia_invalida AS (
  SELECT DISTINCT
    data_ordem,
    id_ordem_pagamento
  FROM
    `rj-smtr`.`validacao_dados_jae_staging`.`ordem_pagamento_consorcio_dia_invalida`
  
    WHERE
      data_ordem = DATE("2022-01-01T01:00:00")
  
),
dia_invalida AS (
  SELECT
    data_ordem,
    id_ordem_pagamento
  FROM
    `rj-smtr`.`validacao_dados_jae_staging`.`ordem_pagamento_dia_invalida`
  
    WHERE
      data_ordem = DATE("2022-01-01T01:00:00")
  
),
indicadores AS (
  SELECT
    data_ordem,
    id_ordem_pagamento,
    CASE
      WHEN sod.id_ordem_pagamento IS NOT NULL THEN sod.indicador_captura_invalida
      ELSE FALSE
    END AS indicador_captura_invalida,
    CASE
      WHEN sod.id_ordem_pagamento IS NOT NULL THEN sod.indicador_servico_fora_vigencia
      ELSE FALSE
    END AS indicador_servico_fora_vigencia,
    cod.id_ordem_pagamento IS NOT NULL AS indicador_agregacao_consorcio_operador_dia_invalida,
    cd.id_ordem_pagamento IS NOT NULL AS indicador_agregacao_consorcio_dia_invalida,
    d.id_ordem_pagamento IS NOT NULL AS indicador_agregacao_dia_invalida,
  FROM
    dia_invalida d
  FULL OUTER JOIN
    servico_operador_dia_invalida sod
  USING(data_ordem, id_ordem_pagamento)
  FULL OUTER JOIN
    consorcio_operador_dia_invalida cod
  USING(data_ordem, id_ordem_pagamento)
  FULL OUTER JOIN
    consorcio_dia_invalida cd
  USING(data_ordem, id_ordem_pagamento)
)
SELECT
  *,
  '' AS versao
FROM
  indicadores
WHERE
  indicador_captura_invalida
  OR indicador_servico_fora_vigencia
  OR indicador_agregacao_consorcio_operador_dia_invalida
  OR indicador_agregacao_consorcio_dia_invalida
  OR indicador_agregacao_dia_invalida