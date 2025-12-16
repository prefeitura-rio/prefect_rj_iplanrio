

with __dbt__cte__aux_passageiros_hora as (


SELECT
  data,
  hora,
  modo,
  consorcio,
  id_servico_jae,
  servico_jae,
  descricao_servico_jae,
  sentido,
  id_transacao,
  tipo_transacao_smtr,
  CASE
    WHEN tipo_transacao_smtr = "Gratuidade" THEN tipo_gratuidade
    WHEN tipo_transacao_smtr = "Integração" THEN "Integração"
    WHEN tipo_transacao_smtr = "Transferência" THEN "Transferência"
    ELSE tipo_pagamento
  END AS tipo_transacao_detalhe_smtr,
  tipo_gratuidade,
  tipo_pagamento,
  geo_point_transacao
FROM
  `rj-smtr`.`br_rj_riodejaneiro_bilhetagem`.`transacao`
WHERE
  id_servico_jae NOT IN ("140", "142")
  AND id_operadora != "2"
  AND (
    modo = "BRT"
    OR (modo = "VLT" AND data >= DATE("2024-02-24"))
    OR (modo = "Ônibus" AND data >= DATE("2024-04-19"))
    OR (modo = "Van" AND consorcio = "STPC" AND data >= DATE("2024-07-01"))
    OR (modo = "Van" AND consorcio = "STPL" AND data >= DATE("2024-07-15"))
  )
  AND tipo_transacao IS NOT NULL

UNION ALL

SELECT
  data,
  hora,
  modo,
  consorcio,
  id_servico_jae,
  servico_jae,
  descricao_servico_jae,
  sentido,
  id_transacao,
  "RioCard" AS tipo_transacao_smtr,
  "RioCard" AS tipo_transacao_detalhe_smtr,
  NULL AS tipo_gratuidade,
  "RioCard" AS tipo_pagamento,
  ST_GEOGPOINT(longitude, latitude) AS geo_point_transacao
FROM
  `rj-smtr`.`br_rj_riodejaneiro_bilhetagem`.`transacao_riocard`
WHERE
  (id_servico_jae NOT IN ("140", "142") OR id_servico_jae IS NULL)
  AND (id_operadora != "2" OR id_operadora IS NULL)
  AND (
    modo = "BRT"
    OR (modo = "VLT" AND data >= DATE("2024-02-24"))
    OR (modo = "Ônibus" AND data >= DATE("2024-04-19"))
    OR (modo = "Van" AND consorcio = "STPC" AND data >= DATE("2024-07-01"))
    OR (modo = "Van" AND consorcio = "STPL" AND data >= DATE("2024-07-15"))
    OR modo IS NULL
  )
) /*
consulta as partições a serem atualizadas com base nas transações capturadas entre date_range_start e date_range_end
e as integrações capturadas entre date_range_start e date_range_end
*/



  
    -- Transações Jaé
    

    

    
  


SELECT
  p.* EXCEPT(id_transacao, geo_point_transacao),
  geo.tile_id,
  COUNT(id_transacao) AS quantidade_passageiros,
  '' AS versao
FROM
  __dbt__cte__aux_passageiros_hora p
JOIN
  `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`aux_h3_res9` geo
ON
  ST_CONTAINS(geo.geometry, geo_point_transacao)
WHERE

  
    data = "2000-01-01"
  

GROUP BY
  data,
  hora,
  modo,
  consorcio,
  id_servico_jae,
  servico_jae,
  descricao_servico_jae,
  sentido,
  tipo_transacao_smtr,
  tipo_transacao_detalhe_smtr,
  tipo_gratuidade,
  tipo_pagamento,
  tile_id