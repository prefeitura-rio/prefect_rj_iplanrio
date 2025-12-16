-- depends_on: `rj-smtr`.`veiculo_staging`.`sppo_licenciamento_stu`



  


WITH
  licenciamento AS (
  SELECT
    DATE("2022-01-01T01:00:00") AS data,
    id_veiculo,
    placa,
    tipo_veiculo,
    indicador_ar_condicionado,
    TRUE AS indicador_licenciado,
    CASE
    WHEN ano_ultima_vistoria_atualizado >= CAST(EXTRACT(YEAR FROM DATE_SUB(DATE("2022-01-01T01:00:00"), INTERVAL 1 YEAR)) AS INT64) THEN TRUE -- Última vistoria realizada dentro do período válido
    WHEN data_ultima_vistoria IS NULL AND DATE_DIFF(DATE("2022-01-01T01:00:00"), data_inicio_vinculo, DAY) <=  15 THEN TRUE -- Caso o veículo seja novo, existe a tolerância de 15 dias para a primeira vistoria
    WHEN ano_fabricacao IN (2023, 2024) AND CAST(EXTRACT(YEAR FROM DATE("2022-01-01T01:00:00")) AS INT64) = 2024 THEN TRUE -- Caso o veículo tiver ano de fabricação 2023 ou 2024, será considerado como vistoriado apenas em 2024 (regra de transição)
  ELSE FALSE
  END AS indicador_vistoriado,
  FROM
    `rj-smtr`.`veiculo`.`sppo_licenciamento` --`rj-smtr`.`veiculo`.`sppo_licenciamento`
  WHERE
    data = DATE("2023-02-01")
  ),
  gps AS (
  SELECT
    DISTINCT data,
    id_veiculo
  FROM
    `rj-smtr`.`br_rj_riodejaneiro_veiculos`.`gps_sppo`
    -- rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo
  WHERE
    data = DATE("2022-01-01T01:00:00") ),
  autuacoes AS (
  SELECT
    DISTINCT data_infracao AS data,
    placa,
    id_infracao
  FROM
    `rj-smtr`.`veiculo`.`sppo_infracao`
  WHERE
    
  data = DATE("2023-02-10")
    AND data_infracao = DATE("2022-01-01T01:00:00")
    AND modo = "ONIBUS"),
  registros_agente_verao AS (
    SELECT
      DISTINCT data,
      id_veiculo,
      TRUE AS indicador_registro_agente_verao_ar_condicionado
    FROM
      `rj-smtr`.`veiculo`.`sppo_registro_agente_verao`
      -- rj-smtr.veiculo.sppo_registro_agente_verao
    WHERE
      data = DATE("2022-01-01T01:00:00") ),
  autuacao_ar_condicionado AS (
  SELECT
    data,
    placa,
    TRUE AS indicador_autuacao_ar_condicionado
  FROM
    autuacoes
  WHERE
    id_infracao = "023.II" ),
  autuacao_seguranca AS (
  SELECT
    data,
    placa,
    TRUE AS indicador_autuacao_seguranca
  FROM
    autuacoes
  WHERE
    id_infracao IN (
      "016.VI",
      "023.VII",
      "024.II",
      "024.III",
      "024.IV",
      "024.V",
      "024.VI",
      "024.VII",
      "024.VIII",
      "024.IX",
      "024.XII",
      "024.XIV",
      "024.XV",
      "025.II",
      "025.XII",
      "025.XIII",
      "025.XIV",
      "026.X") ),
  autuacao_equipamento AS (
  SELECT
    data,
    placa,
    TRUE AS indicador_autuacao_equipamento
  FROM
    autuacoes
  WHERE
    id_infracao IN (
      "023.IV",
      "023.V",
      "023.VI",
      "023.VIII",
      "024.XIII",
      "024.XI",
      "024.XVIII",
      "024.XXI",
      "025.III",
      "025.IV",
      "025.V",
      "025.VI",
      "025.VII",
      "025.VIII",
      "025.IX",
      "025.X",
      "025.XI") ),
  autuacao_limpeza AS (
  SELECT
    data,
    placa,
    TRUE AS indicador_autuacao_limpeza
  FROM
    autuacoes
  WHERE
    id_infracao IN (
      "023.IX",
      "024.X") ),
  autuacoes_agg AS (
  SELECT
    DISTINCT *
  FROM
    autuacao_ar_condicionado
  FULL JOIN
    autuacao_seguranca
  USING
    (data,
      placa)
  FULL JOIN
    autuacao_equipamento
  USING
    (data,
      placa)
  FULL JOIN
    autuacao_limpeza
  USING
    (data,
      placa) ),
  gps_licenciamento_autuacao AS (
  SELECT
    data,
    id_veiculo,
    
      STRUCT( COALESCE(l.indicador_licenciado, FALSE)                     AS indicador_licenciado,
              COALESCE(l.indicador_ar_condicionado, FALSE)                AS indicador_ar_condicionado,
              COALESCE(a.indicador_autuacao_ar_condicionado, FALSE)       AS indicador_autuacao_ar_condicionado,
              COALESCE(a.indicador_autuacao_seguranca, FALSE)             AS indicador_autuacao_seguranca,
              COALESCE(a.indicador_autuacao_limpeza, FALSE)               AS indicador_autuacao_limpeza,
              COALESCE(a.indicador_autuacao_equipamento, FALSE)           AS indicador_autuacao_equipamento,
              COALESCE(r.indicador_registro_agente_verao_ar_condicionado, FALSE)   AS indicador_registro_agente_verao_ar_condicionado)
    
    AS indicadores
  FROM
    gps g
  LEFT JOIN
    licenciamento AS l
  USING
    (data,
      id_veiculo)
  LEFT JOIN
    autuacoes_agg AS a
  USING
    (data,
      placa)
  LEFT JOIN
    registros_agente_verao AS r
  USING
    (data,
      id_veiculo))

SELECT
  gla.* EXCEPT(indicadores),
  TO_JSON(indicadores) AS indicadores,
  status,
  "" AS versao
FROM
  gps_licenciamento_autuacao AS gla
LEFT JOIN
  `rj-smtr`.`dashboard_subsidio_sppo`.`subsidio_parametros` AS p --`rj-smtr.dashboard_subsidio_sppo.subsidio_parametros`
ON
  gla.indicadores.indicador_licenciado = p.indicador_licenciado
  AND gla.indicadores.indicador_ar_condicionado = p.indicador_ar_condicionado
  AND gla.indicadores.indicador_autuacao_ar_condicionado = p.indicador_autuacao_ar_condicionado
  AND gla.indicadores.indicador_autuacao_seguranca = p.indicador_autuacao_seguranca
  AND gla.indicadores.indicador_autuacao_limpeza = p.indicador_autuacao_limpeza
  AND gla.indicadores.indicador_autuacao_equipamento = p.indicador_autuacao_equipamento
  AND gla.indicadores.indicador_registro_agente_verao_ar_condicionado = p.indicador_registro_agente_verao_ar_condicionado
  AND (data BETWEEN p.data_inicio AND p.data_fim)
