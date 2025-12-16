





    
        

        
    


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
),  __dbt__cte__aux_trips_dia as (



with
    routes as (
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
),  __dbt__cte__aux_frequencies_horario_tratado as (


with
    frequencies as (
        select
            *,
            split(start_time, ":") as start_time_parts,
            split(end_time, ":") as end_time_parts,
        
        from `rj-smtr`.`gtfs`.`frequencies`
    )

select
    * except (start_time_parts, end_time_parts, start_time, end_time),
    make_interval(
        hour => cast(start_time_parts[0] as integer),
        minute => cast(start_time_parts[1] as integer),
        second => cast(start_time_parts[2] as integer)
    ) as start_time,
    make_interval(
        hour => cast(end_time_parts[0] as integer),
        minute => cast(end_time_parts[1] as integer),
        second => cast(end_time_parts[2] as integer)
    ) as end_time
from frequencies
),  __dbt__cte__aux_stop_times_horario_tratado as (


with
    stop_times as (
        select *, split(arrival_time, ":") as arrival_time_parts,
        
        from `rj-smtr`.`gtfs`.`stop_times`
    )

select
    * except (arrival_time, arrival_time_parts),
    make_interval(
        hour => cast(arrival_time_parts[0] as integer),
        minute => cast(arrival_time_parts[1] as integer),
        second => cast(arrival_time_parts[2] as integer)
    ) as arrival_time
from stop_times
), trips_dia as (
        select *
        from __dbt__cte__aux_trips_dia
        where
            feed_start_date >= '2024-09-29'
            
                and feed_start_date in ()
                and data between date("2022-01-01T00:00:00") and date(
                    "2022-01-01T01:00:00"
                )
            
    ),
    frequencies_tratada as (
        select *
        from __dbt__cte__aux_frequencies_horario_tratado
        where
            feed_start_date >= '2024-09-29'
            
                and feed_start_date in ()
            
    ),
    trips_frequences_dia as (
        select
            td.*,
            timestamp(data + start_time, "America/Sao_Paulo") as start_timestamp,
            timestamp(data + end_time, "America/Sao_Paulo") as end_timestamp,
            f.headway_secs
        from trips_dia td
        join frequencies_tratada f using (feed_start_date, feed_version, trip_id)
    ),
    trips_alternativas as (
        select
            data,
            servico,
            direction_id,
            array_agg(
                struct(
                    trip_id as trip_id,
                    shape_id as shape_id,
                    evento as evento,
                    extensao as extensao
                )
            ) as trajetos_alternativos
        from trips_dia td
        where td.trip_id not in (select trip_id from frequencies_tratada)
        group by 1, 2, 3
    ),
    viagens_frequencies as (
        select
            tfd.* except (start_timestamp, end_timestamp, headway_secs),
            datetime(partida, "America/Sao_Paulo") as datetime_partida
        from
            trips_frequences_dia tfd,
            unnest(
                generate_timestamp_array(
                    start_timestamp,
                    timestamp_sub(end_timestamp, interval 1 second),
                    interval headway_secs second
                )
            ) as partida
    ),
    viagens_stop_times as (
        select
            td.data,
            trip_id,
            td.modo,
            td.route_id,
            td.service_id,
            td.servico,
            td.direction_id,
            td.shape_id,
            td.tipo_dia,
            td.subtipo_dia,
            td.tipo_os,
            feed_version,
            feed_start_date,
            td.evento,
            td.extensao,
            td.distancia_total_planejada,
            td.indicador_possui_os,
            td.horario_inicio,
            td.horario_fim,
            td.data + st.arrival_time as datetime_partida
        from trips_dia td
        join
            __dbt__cte__aux_stop_times_horario_tratado st using (
                feed_start_date, feed_version, trip_id
            )
        left join frequencies_tratada f using (feed_start_date, feed_version, trip_id)
        where
            feed_start_date >= '2024-09-29'
            
                and feed_start_date in ()
            
            and st.stop_sequence = 0
            and f.trip_id is null
    ),
    viagens_trips_alternativas as (
        select v.*, ta.trajetos_alternativos
        from
            (
                select *
                from viagens_frequencies
                union all
                select *
                from viagens_stop_times
            ) v
        left join trips_alternativas ta using (data, servico, direction_id)
    ),
    viagem_filtrada as (
        -- filtra viagens fora do horario de inicio e fim e em dias não previstos na OS
        select *
        from viagens_trips_alternativas
        where
            (distancia_total_planejada is null or distancia_total_planejada > 0)
            and (
                not indicador_possui_os
                or horario_inicio is null
                or horario_fim is null
                or datetime_partida between data + horario_inicio and data + horario_fim
            )
    ),
    servico_circular as (
        select feed_start_date, feed_version, shape_id
        
        from `rj-smtr`.`planejamento`.`shapes_geom`
        where
            feed_start_date >= '2024-09-29'
            
                and feed_start_date in ()
            
            and round(st_y(start_pt), 4) = round(st_y(end_pt), 4)
            and round(st_x(start_pt), 4) = round(st_x(end_pt), 4)
    ),
    viagem_planejada as (
        select
            date(datetime_partida) as data,
            datetime_partida,
            modo,
            service_id,
            trip_id,
            route_id,
            shape_id,
            servico,
            case
                when c.shape_id is not null
                then "C"
                when direction_id = '0'
                then "I"
                else "V"
            end as sentido,
            evento,
            extensao,
            trajetos_alternativos,
            data as data_referencia,
            tipo_dia,
            subtipo_dia,
            tipo_os,
            feed_version,
            feed_start_date,
            '' as versao,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
        from viagem_filtrada v
        left join servico_circular c using (shape_id, feed_version, feed_start_date)
    ),
    viagem_planejada_id as (
        select
            *,
            concat(
                servico,
                "_",
                sentido,
                "_",
                shape_id,
                "_",
                format_datetime("%Y%m%d%H%M%S", datetime_partida)
            ) as id_viagem
        from viagem_planejada
    )
select data, id_viagem, * except (data, id_viagem, rn)
from
    (
        select
            *,
            row_number() over (
                partition by id_viagem order by data_referencia desc
            ) as rn
        from viagem_planejada_id
    )
where rn = 1 and data is not null