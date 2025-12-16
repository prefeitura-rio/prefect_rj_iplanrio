





  

  


WITH staging_transacao AS (
  SELECT
    *
  FROM
    `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`transacao_riocard`
  
    WHERE
      
  DATE(data) BETWEEN DATE("2022-01-01T00:00:00") AND DATE("2022-01-01T01:00:00")
  AND timestamp_captura BETWEEN DATETIME("2022-01-01T00:00:00") AND DATETIME("2022-01-01T01:00:00")

  
),
novos_dados AS (
  SELECT
    EXTRACT(DATE FROM t.data_transacao) AS data,
    EXTRACT(HOUR FROM t.data_transacao) AS hora,
    t.data_transacao AS datetime_transacao,
    t.data_processamento AS datetime_processamento,
    t.timestamp_captura AS datetime_captura,
    COALESCE(do.modo, dc.modo) AS modo,
    dc.id_consorcio,
    dc.consorcio,
    do.id_operadora,
    do.operadora,
    t.cd_linha AS id_servico_jae,
    l.nr_linha AS servico_jae,
    l.nm_linha AS descricao_servico_jae,
    t.sentido,
    CASE
      WHEN do.modo = "VLT" THEN SUBSTRING(t.veiculo_id, 1, 3)
      WHEN do.modo = "BRT" THEN NULL
      ELSE t.veiculo_id
    END AS id_veiculo,
    t.numero_serie_validador AS id_validador,
    t.id AS id_transacao,
    t.latitude_trx AS latitude,
    t.longitude_trx AS longitude,
    ST_GEOGPOINT(t.longitude_trx, t.latitude_trx) AS geo_point_transacao,
    t.valor_transacao
  FROM
    staging_transacao t
  LEFT JOIN
    `rj-smtr`.`cadastro`.`operadoras` do
  ON
    t.cd_operadora = do.id_operadora_jae
  LEFT JOIN
    `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`linha` l
  ON
    t.cd_linha = l.cd_linha
  LEFT JOIN
    `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`linha_consorcio` lc
  ON
    t.cd_linha = lc.cd_linha
    AND (
      t.data_transacao BETWEEN lc.dt_inicio_validade AND lc.dt_fim_validade
      OR lc.dt_fim_validade IS NULL
    )
  LEFT JOIN
    `rj-smtr`.`cadastro`.`consorcios` dc
  ON
    lc.cd_consorcio = dc.id_consorcio_jae
),
-- consorcios AS (
--   SELECT
--     t.data,
--     t.hora,
--     t.datetime_transacao,
--     t.datetime_processamento,
--     t.datetime_captura,
--     COALESCE(t.modo, dc.modo) AS modo,
--     dc.id_consorcio,
--     dc.consorcio,
--     t.id_operadora,
--     t.operadora,
--     t.id_servico_jae,
--     t.servico_jae,
--     t.descricao_servico_jae,
--     t.sentido,
--     t.id_veiculo,
--     t.id_validador,
--     t.id_transacao,
--     t.latitude,
--     t.longitude,
--     t.valor_transacao
--   FROM
--     novos_dados t
--   LEFT JOIN
--     `rj-smtr`.`cadastro`.`consorcios` dc
--   USING(id_consorcio_jae)
-- ),
particoes_completas AS (
  SELECT
    *,
    0 AS priority
  FROM
    novos_dados

  
),
transacao_deduplicada AS (
  SELECT
    * EXCEPT(rn, priority)
  FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY id_transacao ORDER BY datetime_captura DESC, priority) AS rn
    FROM
      particoes_completas
  )
  WHERE
    rn = 1
)
SELECT
  *,
  '' AS versao
FROM
  transacao_deduplicada