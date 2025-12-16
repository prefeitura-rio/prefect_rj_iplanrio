


with
     __dbt__cte__aux_ordem_servico_diaria as (


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
),  __dbt__cte__aux_ordem_servico_horario_tratado as (


with
    ordem_servico as (
        select
            * except (horario_inicio, horario_fim),
            split(horario_inicio, ":") horario_inicio_parts,
            split(horario_fim, ":") horario_fim_parts
        from __dbt__cte__aux_ordem_servico_diaria
    )
select
    * except (horario_fim_parts, horario_inicio_parts),
    case
        when array_length(horario_inicio_parts) = 3
        then
            make_interval(
                hour => cast(horario_inicio_parts[0] as integer),
                minute => cast(horario_inicio_parts[1] as integer),
                second => cast(horario_inicio_parts[2] as integer)
            )
    end as horario_inicio,
    case
        when array_length(horario_fim_parts) = 3
        then
            make_interval(
                hour => cast(horario_fim_parts[0] as integer),
                minute => cast(horario_fim_parts[1] as integer),
                second => cast(horario_fim_parts[2] as integer)
            )
    end as horario_fim
from ordem_servico
), routes as (
        select
            *,
            case
                when agency_id in ("22005", "22002", "22004", "22003")
                then "Ônibus"
                when agency_id = "20001"
                then "BRT"
            end as modo
        
        from `rj-smtr`.`gtfs`.`routes`
    ),
    trips_dia as (
        select
            c.data,
            t.trip_id,
            r.modo,
            t.route_id,
            t.service_id,
            r.route_short_name as servico,
            t.direction_id,
            t.shape_id,
            c.tipo_dia,
            c.subtipo_dia,
            c.tipo_os,
            t.feed_version,
            t.feed_start_date,
            regexp_extract(t.trip_headsign, r'\[.*?\]') as evento
        
        from `rj-smtr`.`planejamento`.`calendario` c
        
        join `rj-smtr`.`gtfs`.`trips` t using (feed_start_date, feed_version)
        join routes r using (feed_start_date, feed_version, route_id)
        where t.service_id in unnest(c.service_ids)
    )
select
    td.* except (evento),
    osa.evento,
    case
        when td.direction_id = '0'
        then ifnull(osa.extensao_ida, os.extensao_ida)
        when td.direction_id = '1'
        then ifnull(osa.extensao_volta, os.extensao_volta)
    end as extensao,
    os.distancia_total_planejada,
    os.feed_start_date is not null as indicador_possui_os,
    os.horario_inicio,
    os.horario_fim,
from trips_dia td
left join
    `rj-smtr`.`gtfs`.`ordem_servico_trajeto_alternativo` osa using (
        feed_start_date, feed_version, tipo_os, servico, evento
    )

left join
    __dbt__cte__aux_ordem_servico_horario_tratado os using (
        feed_start_date, feed_version, tipo_os, tipo_dia, servico
    )