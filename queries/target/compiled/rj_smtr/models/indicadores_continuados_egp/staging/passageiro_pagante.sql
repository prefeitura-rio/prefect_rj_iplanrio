

WITH consorcio AS (
  SELECT
    id_consorcio,
    modo
  FROM
    `rj-smtr`.`cadastro`.`consorcios`
    -- rj-smtr.cadastro.consorcios
  WHERE
    modo IN ("Ônibus", "BRT")
)
SELECT
  DATE_TRUNC(data, MONTH) AS data,
  rdo.ano,
  rdo.mes,
  c.modo,
  SUM(qtd_buc_1_perna+qtd_buc_2_perna_integracao+
      qtd_buc_supervia_1_perna+qtd_buc_supervia_2_perna_integracao+
      qtd_cartoes_perna_unica_e_demais+qtd_pagamentos_especie) AS quantidade_passageiro_pagante_mes,
  CURRENT_DATE("America/Sao_Paulo") AS data_ultima_atualizacao,
  '' as versao
FROM
  consorcio AS c
LEFT JOIN
  `rj-smtr`.`br_rj_riodejaneiro_rdo`.`rdo40_tratado` AS rdo
ON
  rdo.termo = c.id_consorcio
WHERE
  rdo.data >= "2015-01-01"
  
  AND rdo.data BETWEEN DATE_TRUNC(DATE("2022-01-01T01:00:00"), MONTH)
  AND LAST_DAY(DATE("2022-01-01T01:00:00"), MONTH)
  AND rdo.data < DATE_TRUNC(CURRENT_DATE("America/Sao_Paulo"), MONTH)
  
GROUP BY
  data,
  rdo.ano,
  rdo.mes,
  c.modo