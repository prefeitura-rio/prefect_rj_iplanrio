
  WITH
-- 1. Viagens planejadas (agrupadas por data e serviço)
  planejado AS (
  SELECT
    DISTINCT data,
    tipo_dia,
    consorcio,
    servico,
    faixa_horaria_inicio,
    faixa_horaria_fim,
    partidas_total_planejada,
    distancia_total_planejada AS km_planejada,
  FROM
    `rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada`
    -- rj-smtr.projeto_subsidio_sppo.viagem_planejada
  WHERE
    data BETWEEN DATE("2022-01-01T01:00:00")
    AND DATE( "2022-01-01T01:00:00" )
    AND ( distancia_total_planejada > 0
      OR distancia_total_planejada IS NULL )
    AND (id_tipo_trajeto = 0
      OR id_tipo_trajeto IS NULL)
  ),
  viagens_planejadas AS (
  SELECT
    feed_start_date,
    servico,
    tipo_dia,
    viagens_planejadas,
    partidas_ida,
    partidas_volta,
    tipo_os,
  FROM
      `rj-smtr`.`gtfs`.`ordem_servico`
      -- rj-smtr.gtfs.ordem_servico
  WHERE
    feed_start_date IN ('')
  ),
  data_versao_efetiva AS (
  SELECT
    data,
    tipo_dia,
    tipo_os,
    COALESCE(feed_start_date, data_versao_trips, data_versao_shapes, data_versao_frequencies) AS feed_start_date
  FROM
      `rj-smtr`.`projeto_subsidio_sppo`.`subsidio_data_versao_efetiva`
      -- rj-smtr.projeto_subsidio_sppo.subsidio_data_versao_efetiva -- (alterar também query no bloco execute)
  WHERE
    data BETWEEN DATE("2022-01-01T01:00:00")
    AND DATE( "2022-01-01T01:00:00" )
  ),
  viagem_planejada AS (
  SELECT
    p.data,
    p.tipo_dia,
    p.consorcio,
    p.servico,
    p.faixa_horaria_inicio,
    p.faixa_horaria_fim,
    v.viagens_planejadas,
    p.km_planejada,
    IF(p.data >= DATE("2024-08-16"), p.partidas_total_planejada, v.partidas_ida + v.partidas_volta) AS viagens_planejadas_ida_volta
  FROM
    planejado AS p
  LEFT JOIN
    data_versao_efetiva AS d
  USING
    (data, tipo_dia)
  LEFT JOIN
    viagens_planejadas AS v
  ON
    d.feed_start_date = v.feed_start_date
    AND p.tipo_dia = v.tipo_dia
    AND p.servico = v.servico
    AND (d.tipo_os = v.tipo_os
      OR (d.tipo_os IS NULL AND v.tipo_os = "Regular"))
  ),
-- 2. Parâmetros de subsídio
  subsidio_parametros AS (
  SELECT
    DISTINCT data_inicio,
    data_fim,
    status,
    subsidio_km,
    MAX(subsidio_km) OVER (PARTITION BY DATE_TRUNC(data_inicio, YEAR), data_fim) AS subsidio_km_teto,
    indicador_penalidade_judicial
  FROM
    `rj-smtr`.`dashboard_subsidio_sppo_staging`.`subsidio_valor_km_tipo_viagem`
    -- rj-smtr-staging.dashboard_subsidio_sppo_staging.subsidio_valor_km_tipo_viagem
),
-- 3. Viagens com quantidades de transações
  viagem_transacao AS (
  SELECT
    *
 FROM
    `rj-smtr`.`subsidio`.`viagem_transacao`
    -- rj-smtr.subsidio.viagem_transacao
  WHERE
    data BETWEEN DATE("2022-01-01T01:00:00")
    AND DATE( "2022-01-01T01:00:00" )
  ),
-- 4. Viagens com tipo e valor de subsídio por km
  viagem_km_tipo AS (
  SELECT
    vt.data,
    vt.servico,
    vt.tipo_viagem,
    vt.id_viagem,
    vt.datetime_partida,
    vt.distancia_planejada,
    t.subsidio_km,
    t.subsidio_km_teto,
    t.indicador_penalidade_judicial
  FROM
    viagem_transacao AS vt
  LEFT JOIN
    subsidio_parametros AS t
  ON
    vt.data BETWEEN t.data_inicio
    AND t.data_fim
    AND vt.tipo_viagem = t.status ),
-- 5. Apuração de km realizado e Percentual de Operação Diário (POD)
  servico_faixa_km_apuracao AS (
  SELECT
    p.data,
    p.tipo_dia,
    p.faixa_horaria_inicio,
    p.faixa_horaria_fim,
    p.consorcio,
    p.servico,
    p.km_planejada AS km_planejada,
    COALESCE(ROUND(100 * SUM(IF(v.tipo_viagem NOT IN ("Não licenciado","Não vistoriado"),v.distancia_planejada, 0)) / p.km_planejada,2), 0) AS pof
  FROM
    viagem_planejada AS p
  LEFT JOIN
    viagem_km_tipo AS v
  ON
    p.data = v.data
    AND p.servico = v.servico
    AND v.datetime_partida BETWEEN p.faixa_horaria_inicio
    AND p.faixa_horaria_fim
  GROUP BY
    1, 2, 3, 4, 5, 6, 7
  )
-- 6. Flag de viagens que serão consideradas ou não para fins de remuneração (apuração de valor de subsídio) - RESOLUÇÃO SMTR Nº 3645/2023
SELECT
  v.* EXCEPT(rn, datetime_partida, viagens_planejadas, viagens_planejadas_ida_volta, km_planejada, tipo_dia, consorcio, faixa_horaria_inicio, faixa_horaria_fim),
  CASE
    WHEN v.data >= DATE("2023-09-16")
        AND v.tipo_dia = "Dia Útil"
        AND viagens_planejadas > 10
        AND pof > 120
        AND rn > viagens_planejadas_ida_volta*1.2
        THEN FALSE
    WHEN v.data >= DATE("2023-09-16")
        AND v.tipo_dia = "Dia Útil"
        AND viagens_planejadas <= 10
        AND pof > 200
        AND rn > viagens_planejadas_ida_volta*2
        THEN FALSE
    WHEN v.data >= DATE("2023-09-16")
        AND (v.tipo_dia = "Dia Útil"
          AND (viagens_planejadas IS NULL
            OR pof IS NULL
            OR rn IS NULL
          )
        )
      THEN NULL
    ELSE
        TRUE
    END AS indicador_viagem_dentro_limite
FROM (
SELECT
  v.*,
  p.* EXCEPT(data, servico),
  ROW_NUMBER() OVER(PARTITION BY v.data, v.servico, faixa_horaria_inicio, faixa_horaria_fim ORDER BY subsidio_km*distancia_planejada DESC) AS rn
FROM
  viagem_km_tipo AS v
LEFT JOIN
  viagem_planejada AS p
ON
  p.data = v.data
  AND p.servico = v.servico
  AND v.datetime_partida BETWEEN p.faixa_horaria_inicio
  AND p.faixa_horaria_fim
) AS v
LEFT JOIN
  servico_faixa_km_apuracao AS s
ON
  s.data = v.data
  AND s.servico = v.servico
  AND v.datetime_partida BETWEEN s.faixa_horaria_inicio
  AND s.faixa_horaria_fim