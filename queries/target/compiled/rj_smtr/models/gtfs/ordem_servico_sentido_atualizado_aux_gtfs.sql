/*
  ordem_servico_gtfs com sentidos despivotados, ajustes nos horários e com atualização dos sentidos circulares
*/


with
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
          
          trip_headsign LIKE "%[desvio_obra]%" OR
          
          trip_headsign LIKE "%[desvio_lazer]%" OR
          
          trip_headsign LIKE "%[desvio_januário]%" OR
          
          trip_headsign LIKE "%[excepcionalidade]%" OR
          
          trip_headsign LIKE "%[excepcionalidade_2]%" OR
          
          trip_headsign LIKE "%[excepcionalidade] 2%" OR
          
          trip_headsign LIKE "%[madonna 2]%" OR
          
          trip_headsign LIKE "%[desvio_túnel]%" OR
          
          trip_headsign LIKE "%[desvio_obras]%" OR
          
          trip_headsign LIKE "%[excepcionalidade] 1%" OR
          
          trip_headsign LIKE "%[madonna 3]%" OR
          
          trip_headsign LIKE "%[rock_in_rio]%" OR
          
          trip_headsign LIKE "%[reveillon]%" OR
          
          trip_headsign LIKE "%[reversível]%" OR
          
          trip_headsign LIKE "%[desvio_feira]%" OR
          
          trip_headsign LIKE "%[desvio_maracanã]%" OR
          
          trip_headsign LIKE "%[excepcionalidade_1]%" OR
          
          trip_headsign LIKE "%[desvio] [feira]%" OR
          
          trip_headsign LIKE "%[madonna]%" OR
          
          trip_headsign LIKE "%[madonna 1]%" OR
          
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
),  __dbt__cte__aux_ordem_servico_diaria as (


select *
from `rj-smtr`.`gtfs`.`ordem_servico`
where feed_start_date < date('2025-04-30')
union all
select
    feed_version,
    feed_start_date,
    feed_end_date,
    tipo_os,
    servico,
    vista,
    consorcio,
    horario_inicio,
    horario_fim,
    extensao_ida,
    extensao_volta,
    partidas_ida_dia as partidas_ida,
    partidas_volta_dia as partidas_volta,
    viagens_dia as viagens_planejadas,
    sum(quilometragem) as distancia_total_planejada,
    tipo_dia,
    versao_modelo
from `rj-smtr`.`planejamento`.`ordem_servico_faixa_horaria`
where feed_start_date >= date('2025-04-30')
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16, 17
), -- 1. Identifica o sentido de cada serviço
    servico_trips_sentido as (
        select distinct *
        from
            (
                select
                    feed_version,
                    trip_short_name as servico,
                    case
                        when
                            round(st_y(start_pt), 4) = round(st_y(end_pt), 4)
                            and round(st_x(start_pt), 4) = round(st_x(end_pt), 4)
                        then "C"
                        when direction_id = "0"
                        then "I"
                        when direction_id = "1"
                        then "V"
                    end as sentido
                from __dbt__cte__trips_filtrada_aux_gtfs
                where indicador_trajeto_alternativo is false
            )
        where sentido = "C"
    ),
    -- 2. Busca principais informações na Ordem de Serviço (OS)
    ordem_servico as (
        select
            * except (horario_inicio, horario_fim),
            horario_inicio as inicio_periodo,
            horario_fim as fim_periodo,
        from __dbt__cte__aux_ordem_servico_diaria
        
    ),
    -- 3. Despivota ordem de serviço por sentido
    ordem_servico_sentido as (
        select *
        from
            ordem_servico unpivot (
                (distancia_planejada, partidas) for sentido in (
                    (extensao_ida, partidas_ida) as "I",
                    (extensao_volta, partidas_volta) as "V"
                )
            )
    )
-- 4. Atualiza sentido dos serviços circulares na ordem de serviço
select o.* except (sentido), coalesce(s.sentido, o.sentido) as sentido
from ordem_servico_sentido as o
left join servico_trips_sentido as s using (feed_version, servico)
where
    distancia_planejada != 0
    and (
        (
            feed_start_date < '2024-08-16'
            and (distancia_total_planejada != 0 and (partidas != 0 or partidas is null))
        )
        or feed_start_date >= '2024-08-16'
    )