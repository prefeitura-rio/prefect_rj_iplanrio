

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
),  __dbt__cte__ordem_servico_trajeto_alternativo_sentido_atualizado_aux_gtfs as (
/*
  ordem_servico_trajeto_alternativo_gtfs com sentidos despivotados e com atualização dos sentidos circulares
*/



-- 1. Busca anexo de trajetos alternativos
WITH
  ordem_servico_trajeto_alternativo AS (
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
), -- 1. Busca os shapes em formato geográfico
  shapes AS (
    SELECT
      *
    FROM
      `rj-smtr`.`gtfs`.`shapes_geom`
    WHERE
      feed_start_date = '2024-05-03'
    ),
  -- 2. Trata a OS, inclui trip_ids e ajusta nomes das colunas
  ordem_servico_tratada AS (
  SELECT
    *
  FROM
    (
      (
      SELECT
        o.feed_version,
        o.feed_start_date,
        o.feed_end_date,
        o.tipo_os,
        o.tipo_dia,
        servico,
        vista,
        consorcio,
        sentido,
        distancia_planejada,
        distancia_total_planejada,
        inicio_periodo,
        fim_periodo,
        trip_id,
        shape_id,
        indicador_trajeto_alternativo
      FROM
        __dbt__cte__ordem_servico_sentido_atualizado_aux_gtfs AS o
      LEFT JOIN
        __dbt__cte__trips_filtrada_aux_gtfs AS t
      ON
        t.feed_version = o.feed_version
        AND o.servico = t.trip_short_name
        AND
          ((o.tipo_dia = t.tipo_dia AND o.tipo_os != "CNU")
          OR (o.tipo_dia = "Ponto Facultativo" AND t.tipo_dia = "Dia Útil" AND o.tipo_os != "CNU")
          OR (o.feed_start_date = "2024-08-16" AND o.tipo_os = "CNU" AND o.tipo_dia = "Domingo" AND t.tipo_dia = "Sabado")) -- Domingo CNU
        AND
          ((o.sentido IN ("I", "C") AND t.direction_id = "0")
          OR (o.sentido = "V" AND t.direction_id = "1"))
      WHERE
        indicador_trajeto_alternativo IS FALSE
      )
    UNION ALL
      (
      SELECT
        o.feed_version,
        o.feed_start_date,
        o.feed_end_date,
        o.tipo_os,
        o.tipo_dia,
        servico,
        o.vista || " " || ot.evento AS vista,
        o.consorcio,
        sentido,
        ot.distancia_planejada,
        distancia_total_planejada,
        COALESCE(ot.inicio_periodo, o.inicio_periodo) AS inicio_periodo,
        COALESCE(ot.fim_periodo, o.fim_periodo) AS fim_periodo,
        trip_id,
        shape_id,
        indicador_trajeto_alternativo
      FROM
        __dbt__cte__ordem_servico_trajeto_alternativo_sentido_atualizado_aux_gtfs AS ot
      LEFT JOIN
        __dbt__cte__ordem_servico_sentido_atualizado_aux_gtfs AS o
      USING
        (feed_version,
          tipo_os,
          servico,
          sentido)
      LEFT JOIN
        __dbt__cte__trips_filtrada_aux_gtfs AS t
      ON
        t.feed_version = o.feed_version
        AND o.servico = t.trip_short_name
        AND
          (o.tipo_dia = t.tipo_dia
          OR (o.tipo_dia = "Ponto Facultativo" AND t.tipo_dia = "Dia Útil")
          OR (t.tipo_dia = "EXCEP")) -- Inclui trips do service_id/tipo_dia "EXCEP"
        AND
          ((o.sentido IN ("I", "C") AND t.direction_id = "0")
          OR (o.sentido = "V" AND t.direction_id = "1"))
        AND t.trip_headsign LIKE CONCAT("%", ot.evento, "%")
      WHERE
        indicador_trajeto_alternativo IS TRUE
        AND trip_id IS NOT NULL -- Remove serviços de tipo_dia sem planejamento
      )
    )
  ),
  -- 3. Inclui trip_ids de ida e volta para trajetos circulares, ajusta shape_id para trajetos circulares e inclui id_tipo_trajeto
  ordem_servico_trips AS (
    SELECT
      * EXCEPT(shape_id, indicador_trajeto_alternativo),
      shape_id AS shape_id_planejado,
      CASE
        WHEN sentido = "C" THEN shape_id || "_" || SPLIT(trip_id, "_")[OFFSET(1)]
      ELSE
      shape_id
    END
      AS shape_id,
      CASE
        WHEN indicador_trajeto_alternativo IS FALSE THEN 0 -- Trajeto regular
        WHEN indicador_trajeto_alternativo IS TRUE THEN 1 -- Trajeto alternativo
    END
      AS id_tipo_trajeto,
    FROM
    (
      (
        SELECT
          DISTINCT * EXCEPT(trip_id),
          trip_id AS trip_id_planejado,
          trip_id
        FROM
          ordem_servico_tratada
        WHERE
          sentido = "I"
          OR sentido = "V"
      )
      UNION ALL
      (
        SELECT
          * EXCEPT(trip_id),
          trip_id AS trip_id_planejado,
          CONCAT(trip_id, "_0") AS trip_id,
        FROM
          ordem_servico_tratada
        WHERE
          sentido = "C"
      )
      UNION ALL
      (
        SELECT
          * EXCEPT(trip_id),
          trip_id AS trip_id_planejado,
          CONCAT(trip_id, "_1") AS trip_id,
        FROM
          ordem_servico_tratada
        WHERE
          sentido = "C"
      )
    )
  )
SELECT
  feed_version,
  feed_start_date,
  o.feed_end_date,
  tipo_os,
  tipo_dia,
  servico,
  vista,
  o.consorcio,
  sentido,
  CASE
    WHEN feed_start_date >= '2024-08-16' THEN fh.partidas
    ELSE NULL
  END AS partidas_total_planejada,
  distancia_planejada,
  CASE
    WHEN feed_start_date >= '2024-08-16' THEN fh.quilometragem
    ELSE distancia_total_planejada
  END AS distancia_total_planejada,
  inicio_periodo,
  fim_periodo,
  CASE
    WHEN feed_start_date >= '2024-08-16' THEN fh.faixa_horaria_inicio
    ELSE "00:00:00"
  END AS faixa_horaria_inicio,
  CASE
    WHEN feed_start_date >= '2024-08-16' THEN fh.faixa_horaria_fim
    ELSE "23:59:59"
  END AS faixa_horaria_fim,
  trip_id_planejado,
  trip_id,
  shape_id,
  shape_id_planejado,
  shape,
  CASE
    WHEN sentido = "C" AND SPLIT(shape_id, "_")[OFFSET(1)] = "0" THEN "I"
    WHEN sentido = "C" AND SPLIT(shape_id, "_")[OFFSET(1)] = "1" THEN "V"
    WHEN sentido = "I" OR sentido = "V" THEN sentido
END
  AS sentido_shape,
  s.start_pt,
  s.end_pt,
  id_tipo_trajeto,
FROM
  ordem_servico_trips AS o
LEFT JOIN
  shapes AS s
USING
  (feed_version,
    feed_start_date,
    shape_id)
LEFT JOIN
  `rj-smtr`.`planejamento`.`ordem_servico_faixa_horaria` AS fh
  -- rj-smtr-dev.gtfs.ordem_servico_faixa_horaria AS fh
USING
  (feed_version, feed_start_date, tipo_os, tipo_dia, servico)
WHERE
feed_start_date = '2024-05-03' AND
(
    (
    feed_start_date >= '2024-08-16'
    AND
      (
        fh.quilometragem != 0
        AND (fh.partidas != 0 OR fh.partidas IS NULL)
      )
    )
    OR
      feed_start_date < '2024-08-16'
  )