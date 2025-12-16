-- depends_on: `rj-smtr`.`gtfs`.`ordem_servico_trajeto_alternativo`
/*
Identificação de um trip de referência para cada serviço e sentido regular
Identificação de todas as trips de referência para os trajetos alternativos
*/





WITH
  -- 1. Busca os shapes em formato geográfico
  shapes AS (
    SELECT
      *
    FROM
      `rj-smtr`.`gtfs`.`shapes_geom`
    ),
  -- 2. Busca as trips
  trips_all AS (
    SELECT
      *,
      CASE
        WHEN indicador_trajeto_alternativo = TRUE THEN CONCAT(feed_version, trip_short_name, tipo_dia, direction_id, shape_id)
        ELSE CONCAT(feed_version, trip_short_name, tipo_dia, direction_id)
      END AS trip_partition
    FROM
    (
      SELECT
        service_id,
        trip_id,
        trip_headsign,
        trip_short_name,
        direction_id,
        shape_id,
        feed_version,
        shape_distance,
        start_pt,
        end_pt,
        CASE
          WHEN service_id LIKE "%U_%" THEN "Dia Útil"
          WHEN service_id LIKE "%S_%" THEN "Sabado"
          WHEN service_id LIKE "%D_%" THEN "Domingo"
        ELSE
        service_id
      END
        AS tipo_dia,
        CASE WHEN (
          
          trip_headsign LIKE "%[reversível]%" OR
          
          trip_headsign LIKE "%[excepcionalidade]%" OR
          
          trip_headsign LIKE "%[desvio_feira]%" OR
          
          trip_headsign LIKE "%[excepcionalidade_1]%" OR
          
          trip_headsign LIKE "%[excepcionalidade_2]%" OR
          
          trip_headsign LIKE "%[desvio] [feira]%" OR
          
          trip_headsign LIKE "%[excepcionalidade] 1%" OR
          
          trip_headsign LIKE "%[excepcionalidade] 2%" OR
          
          trip_headsign LIKE "%[madonna]%" OR
          
          trip_headsign LIKE "%[madonna 3]%" OR
          
          trip_headsign LIKE "%[madonna 2]%" OR
          
          trip_headsign LIKE "%[madonna 1]%" OR
          
          trip_headsign LIKE "%[desvio_túnel]%" OR
          
          trip_headsign LIKE "%[desvio_lazer]%" OR
          
          trip_headsign LIKE "%[rock_in_rio]%" OR
          
          trip_headsign LIKE "%[desvio_maracanã]%" OR
          
          trip_headsign LIKE "%[desvio_januário]%" OR
          
          service_id = "EXCEP") THEN TRUE
        ELSE FALSE
      END
        AS indicador_trajeto_alternativo,
      FROM
        `rj-smtr`.`gtfs`.`trips`
      LEFT JOIN
        shapes
      USING
        (feed_start_date,
        feed_version,
        shape_id)
      WHERE
        
        service_id NOT LIKE "%_DESAT_%"  -- Desconsidera service_ids desativados
    )
  )
-- 3. Busca as trips de referência para cada serviço, sentido, e tipo_dia
SELECT
    * EXCEPT(rn)
FROM
(
    SELECT
    * EXCEPT(shape_distance),
    ROW_NUMBER() OVER (PARTITION BY trip_partition ORDER BY feed_version, trip_short_name, tipo_dia, direction_id, shape_distance DESC) AS rn
    FROM
    trips_all
)
WHERE
    rn = 1