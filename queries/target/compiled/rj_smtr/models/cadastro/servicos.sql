

with __dbt__cte__aux_routes_vigencia_gtfs as (


WITH routes_rn AS (
  SELECT
    route_id AS id_servico,
    route_short_name AS servico,
    route_long_name AS descricao_servico,
    feed_start_date AS inicio_vigencia,
    feed_end_date AS fim_vigencia,
    LAG(feed_end_date) OVER (PARTITION BY route_id ORDER BY feed_start_date) AS feed_end_date_anterior,
    ROW_NUMBER() OVER (PARTITION BY route_id ORDER BY feed_start_date DESC) AS rn
  FROM
    `rj-smtr`.`gtfs`.`routes`
),
routes_agrupada AS (
  SELECT
    id_servico,
    inicio_vigencia,
    servico,
    descricao_servico,
    IFNULL(fim_vigencia, CURRENT_DATE("America/Sao_Paulo")) as fim_vigencia,
    SUM(
      CASE
        WHEN feed_end_date_anterior IS NULL OR feed_end_date_anterior <> DATE_SUB(inicio_vigencia, INTERVAL 1 DAY) THEN 1
        ELSE 0
      END
    ) OVER (PARTITION BY id_servico ORDER BY inicio_vigencia) AS group_id
  FROM
    routes_rn
),
vigencia AS (
  SELECT
    id_servico,
    MIN(inicio_vigencia) AS inicio_vigencia,
    MAX(fim_vigencia) AS fim_vigencia
  FROM
    routes_agrupada
  GROUP BY
    id_servico,
    group_id
)
SELECT
  id_servico,
  r.servico,
  r.descricao_servico,
  NULL AS latitude,
  NULL AS longitude,
  v.inicio_vigencia,
  CASE
    WHEN v.fim_vigencia != CURRENT_DATE("America/Sao_Paulo") THEN v.fim_vigencia
  END AS fim_vigencia,
  'routes' AS tabela_origem_gtfs,
FROM
 vigencia v
JOIN
(
  SELECT
    id_servico,
    servico,
    descricao_servico
  FROM
    routes_rn
  WHERE
    rn = 1
) r
USING(id_servico)
),  __dbt__cte__aux_stops_vigencia_gtfs as (


WITH stops_rn AS (
  SELECT
    stop_id AS id_servico,
    stop_code AS servico,
    stop_name AS descricao_servico,
    stop_lat AS latitude,
    stop_lon AS longitude,
    feed_start_date AS inicio_vigencia,
    feed_end_date AS fim_vigencia,
    LAG(feed_end_date) OVER (PARTITION BY stop_id ORDER BY feed_start_date) AS feed_end_date_anterior,
    ROW_NUMBER() OVER (PARTITION BY stop_id ORDER BY feed_start_date DESC) AS rn
  FROM
    `rj-smtr`.`gtfs`.`stops`
  WHERE
    location_type = '1'
),
stops_agrupada AS (
  SELECT
    id_servico,
    inicio_vigencia,
    servico,
    descricao_servico,
    IFNULL(fim_vigencia, CURRENT_DATE("America/Sao_Paulo")) AS fim_vigencia,
    SUM(
      CASE
        WHEN feed_end_date_anterior IS NULL OR feed_end_date_anterior <> DATE_SUB(inicio_vigencia, INTERVAL 1 DAY) THEN 1
        ELSE 0
      END
    ) OVER (PARTITION BY id_servico ORDER BY inicio_vigencia) AS group_id
  FROM
    stops_rn
),
vigencia AS (
  SELECT
    id_servico,
    MIN(inicio_vigencia) AS inicio_vigencia,
    MAX(fim_vigencia) AS fim_vigencia
  FROM
    stops_agrupada
  GROUP BY
    id_servico,
    group_id
)
SELECT
  id_servico,
  r.servico,
  r.descricao_servico,
  r.latitude,
  r.longitude,
  v.inicio_vigencia,
  CASE
    WHEN v.fim_vigencia != CURRENT_DATE("America/Sao_Paulo") THEN v.fim_vigencia
  END AS fim_vigencia,
  'stops' AS tabela_origem_gtfs,
FROM
 vigencia v
JOIN
(
  SELECT
    id_servico,
    servico,
    descricao_servico,
    latitude,
    longitude
  FROM
    stops_rn
  WHERE
    rn = 1
) r
USING(id_servico)
),  __dbt__cte__aux_servicos_gtfs as (


SELECT
    id_servico,
    servico,
    descricao_servico,
    latitude,
    longitude,
    inicio_vigencia,
    fim_vigencia,
    tabela_origem_gtfs,
    '' as versao
FROM
    __dbt__cte__aux_routes_vigencia_gtfs

UNION ALL

SELECT
    id_servico,
    servico,
    descricao_servico,
    latitude,
    longitude,
    inicio_vigencia,
    fim_vigencia,
    tabela_origem_gtfs,
    '' as versao
FROM
    __dbt__cte__aux_stops_vigencia_gtfs
) SELECT
    g.id_servico AS id_servico_gtfs,
    j.cd_linha AS id_servico_jae,
    COALESCE(g.servico, j.nr_linha) AS servico,
    g.servico AS servico_gtfs,
    j.nr_linha AS servico_jae,
    COALESCE(g.descricao_servico, j.nm_linha) AS descricao_servico,
    g.descricao_servico AS descricao_servico_gtfs,
    j.nm_linha AS descricao_servico_jae,
    g.latitude,
    g.longitude,
    g.tabela_origem_gtfs,
    COALESCE(g.inicio_vigencia, DATE(j.datetime_inclusao)) AS data_inicio_vigencia,
    g.fim_vigencia AS data_fim_vigencia,
    '' as versao
FROM
    `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`linha` j
FULL OUTER JOIN
    __dbt__cte__aux_servicos_gtfs g
ON
    COALESCE(j.gtfs_route_id, j.gtfs_stop_id) = g.id_servico