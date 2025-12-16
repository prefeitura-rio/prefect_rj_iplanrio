/*
  ordem_servico_trajeto_alternativo_gtfs com sentidos despivotados e com atualização dos sentidos circulares
*/



-- 1. Busca anexo de trajetos alternativos
WITH
   __dbt__cte__trips_filtrada_aux_gtfs as (
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
),  __dbt__cte__ordem_servico_sentido_atualizado_aux_gtfs as (
/*
  ordem_servico_gtfs com sentidos despivotados, ajustes nos horários e com atualização dos sentidos circulares
*/


WITH
  -- 1. Identifica o sentido de cada serviço
  servico_trips_sentido AS (
    SELECT
      DISTINCT *
    FROM
      (
        SELECT
          feed_version,
          trip_short_name AS servico,
          CASE
            WHEN ROUND(ST_Y(start_pt),4) = ROUND(ST_Y(end_pt),4) AND ROUND(ST_X(start_pt),4) = ROUND(ST_X(end_pt),4) THEN "C"
            WHEN direction_id = "0" THEN "I"
            WHEN direction_id = "1" THEN "V"
        END
          AS sentido
        FROM
          __dbt__cte__trips_filtrada_aux_gtfs
        WHERE
          indicador_trajeto_alternativo IS FALSE
      )
    WHERE
      sentido = "C"
  ),
  -- 2. Busca principais informações na Ordem de Serviço (OS)
  ordem_servico AS (
    SELECT
      * EXCEPT(horario_inicio, horario_fim),
      horario_inicio AS inicio_periodo,
      horario_fim  AS fim_periodo,
    FROM
      `rj-smtr`.`gtfs`.`ordem_servico`
    
  ),
  -- 3. Despivota ordem de serviço por sentido
  ordem_servico_sentido AS (
    SELECT
      *
    FROM
      ordem_servico
    UNPIVOT
    (
      (
        distancia_planejada,
        partidas
      ) FOR sentido IN (
        (
          extensao_ida,
          partidas_ida
        ) AS "I",
        (
          extensao_volta,
          partidas_volta
        ) AS "V"
      )
    )
  )
  -- 4. Atualiza sentido dos serviços circulares na ordem de serviço
SELECT
    o.* EXCEPT(sentido),
    COALESCE(s.sentido, o.sentido) AS sentido
FROM
    ordem_servico_sentido AS o
LEFT JOIN
    servico_trips_sentido AS s
USING
    (feed_version, servico)
WHERE
    distancia_planejada != 0
    AND
      (
        (
          feed_start_date < '2024-08-16'
          AND
            (
              distancia_total_planejada != 0
              AND (partidas != 0 OR partidas IS NULL)
            )
        )
      OR
        feed_start_date >= '2024-08-16'
      )
), ordem_servico_trajeto_alternativo AS (
    SELECT
      *
    FROM
      `rj-smtr`.`gtfs`.`ordem_servico_trajeto_alternativo`
    
  ),
  -- 2. Despivota anexo de trajetos alternativos
  ordem_servico_trajeto_alternativo_sentido AS (
    SELECT
      *
    FROM
      ordem_servico_trajeto_alternativo
    UNPIVOT
    (
      (
        distancia_planejada
      ) FOR sentido IN (
        (
          extensao_ida
        ) AS "I",
        (
          extensao_volta
        ) AS "V"
      )
    )
  )
-- 3. Atualiza sentido dos serviços circulares no anexo de trajetos alternativos
SELECT
    * EXCEPT(sentido),
    CASE
        WHEN "C" IN UNNEST(sentido_array) THEN "C"
        ELSE o.sentido
    END AS sentido,
FROM
    ordem_servico_trajeto_alternativo_sentido AS o
LEFT JOIN
(
    SELECT
        feed_start_date,
        servico,
        ARRAY_AGG(DISTINCT sentido) AS sentido_array,
    FROM
        __dbt__cte__ordem_servico_sentido_atualizado_aux_gtfs
    GROUP BY
        1,
        2
) AS s
USING
    (feed_start_date, servico)
WHERE
    distancia_planejada != 0