






    
    


with
     __dbt__cte__aux_os_sppo_faixa_horaria_sentido_dia as (


with
    os as (
        select
            *,
            split(faixa_horaria_inicio, ":") as faixa_horaria_inicio_parts,
            split(faixa_horaria_fim, ":") as faixa_horaria_fim_parts
        
        
        from `rj-smtr`.`planejamento`.`ordem_servico_faixa_horaria`
    
    ),
    os_tratamento_horario as (
        select
            *,
            make_interval(
                hour => cast(faixa_horaria_inicio_parts[0] as integer),
                minute => cast(faixa_horaria_inicio_parts[1] as integer),
                second => cast(faixa_horaria_inicio_parts[2] as integer)
            ) as faixa_horario_intervalo_inicio,
            make_interval(
                hour => cast(faixa_horaria_fim_parts[0] as integer),
                minute => cast(faixa_horaria_fim_parts[1] as integer),
                second => cast(faixa_horaria_fim_parts[2] as integer)
            ) as faixa_horario_intervalo_fim
        from os
    ),
    os_faixa_horaria_dia as (
        select
            c.data,
            feed_version,
            feed_start_date,
            tipo_dia,
            tipo_os,
            servico,
            consorcio,
            extensao_ida,
            extensao_volta,
            viagens_dia,
            c.data + o.faixa_horario_intervalo_inicio as faixa_horaria_inicio,
            c.data + o.faixa_horario_intervalo_fim as faixa_horaria_fim,
            partidas_ida,
            partidas_volta,
            quilometragem,
            partidas
        from `rj-smtr`.`planejamento`.`calendario` c
        
        join
            os_tratamento_horario o using (
                feed_version, feed_start_date, tipo_dia, tipo_os
            )
    ),
    faixas_agregadas as (
        select
            * except (partidas_ida, partidas_volta, partidas, quilometragem),
            case
                when
                    lag(faixa_horaria_inicio) over (
                        partition by servico order by data, faixa_horaria_inicio
                    )
                    = faixa_horaria_inicio
                then
                    lag(partidas_ida) over (
                        partition by servico order by data, faixa_horaria_inicio
                    )
                    + partidas_ida
                else partidas_ida
            end as partidas_ida,
            case
                when
                    lag(faixa_horaria_inicio) over (
                        partition by servico order by data, faixa_horaria_inicio
                    )
                    = faixa_horaria_inicio
                then
                    lag(partidas_volta) over (
                        partition by servico order by data, faixa_horaria_inicio
                    )
                    + partidas_volta
                else partidas_volta
            end as partidas_volta,
            case
                when
                    lag(faixa_horaria_inicio) over (
                        partition by servico order by data, faixa_horaria_inicio
                    )
                    = faixa_horaria_inicio
                then
                    lag(partidas) over (
                        partition by servico order by data, faixa_horaria_inicio
                    )
                    + partidas
                else partidas
            end as partidas,
            case
                when
                    lag(faixa_horaria_inicio) over (
                        partition by servico order by data, faixa_horaria_inicio
                    )
                    = faixa_horaria_inicio
                then
                    lag(quilometragem) over (
                        partition by servico order by data, faixa_horaria_inicio
                    )
                    + quilometragem
                else quilometragem
            end as quilometragem,
            row_number() over (
                partition by servico, faixa_horaria_inicio order by data desc
            ) as rn
        from os_faixa_horaria_dia
    ),
    os_filtrada as (
        select *
        from faixas_agregadas
        where rn = 1 and data = extract(date from faixa_horaria_inicio)
    ),
    os_por_sentido as (
        select
            data,
            feed_version,
            feed_start_date,
            tipo_dia,
            tipo_os,
            servico,
            consorcio,
            sentido.codigo as sentido,
            sentido.extensao as extensao,
            sentido.partidas as partidas,
            quilometragem,  -- TODO: alterar para sentido.extensao * sentido.partidas / 1000 quando subir novo modelo de OS
            faixa_horaria_inicio,
            faixa_horaria_fim
        from
            os_filtrada,
            unnest(
                [
                    struct(
                        'I' as codigo,
                        extensao_ida as extensao,
                        partidas_ida as partidas
                    ),
                    struct(
                        'V' as codigo,
                        extensao_volta as extensao,
                        partidas_volta as partidas
                    )
                ]
            ) as sentido
    )
select *
from os_por_sentido
), os_sppo as (
        select *
        from __dbt__cte__aux_os_sppo_faixa_horaria_sentido_dia
        where
            
    data between
        date('2022-01-01T00:00:00')
        and date('2022-01-01T01:00:00')

            and feed_start_date in ()
    ),
    viagens_planejadas as (
        select
            data,
            modo,
            servico,
            sentido,
            trip_id,
            route_id,
            shape_id,
            evento,
            datetime_partida,
            trajetos_alternativos,
            case
                when evento is not null then true else false
            end as indicador_trajeto_alternativo
        from `rj-smtr`.`planejamento`.`viagem_planejada`
        
        where
            
    data between
        date('2022-01-01T00:00:00')
        and date('2022-01-01T01:00:00')

            and feed_start_date in ()
    ),
    viagens_na_faixa as (
        select
            o.data,
            o.feed_version,
            o.feed_start_date,
            o.tipo_dia,
            o.tipo_os,
            o.servico,
            o.consorcio,
            o.sentido,
            o.extensao,
            o.partidas,
            o.quilometragem,
            o.faixa_horaria_inicio,
            o.faixa_horaria_fim,
            v.modo,
            v.trip_id,
            v.route_id,
            v.shape_id,
            v.evento,
            v.indicador_trajeto_alternativo,
            v.trajetos_alternativos,
            min(datetime_partida) over (
                partition by
                    o.data, o.servico, o.sentido, o.faixa_horaria_inicio, v.trip_id
            ) as primeiro_horario,
            max(datetime_partida) over (
                partition by
                    o.data, o.servico, o.sentido, o.faixa_horaria_inicio, v.trip_id
            ) as ultimo_horario
        from os_sppo o
        left join
            viagens_planejadas v
            on o.data = v.data
            and o.servico = v.servico
            and o.sentido = v.sentido
            and v.datetime_partida
            between o.faixa_horaria_inicio and o.faixa_horaria_fim
    ),
    deduplicado as (
        select
            data,
            feed_version,
            feed_start_date,
            tipo_dia,
            tipo_os,
            servico,
            consorcio,
            sentido,
            extensao,
            partidas,
            quilometragem,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            modo,
            trip_id,
            route_id,
            shape_id,
            evento,
            indicador_trajeto_alternativo,
            trajetos_alternativos,
            primeiro_horario,
            ultimo_horario,
        from viagens_na_faixa
        qualify
            row_number() over (
                partition by
                    data,
                    servico,
                    sentido,
                    faixa_horaria_inicio,
                    trip_id,
                    route_id,
                    shape_id,
                    evento,
                    indicador_trajeto_alternativo
            )
            = 1
    ),
    viagens_agrupadas as (
        select
            data,
            feed_version,
            feed_start_date,
            tipo_dia,
            tipo_os,
            servico,
            consorcio,
            sentido,
            extensao,
            partidas,
            quilometragem,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            modo,
            array_agg(
                struct(
                    primeiro_horario as primeiro_horario,
                    ultimo_horario as ultimo_horario,
                    trip_id as trip_id,
                    route_id as route_id,
                    shape_id as shape_id,
                    evento as evento,
                    indicador_trajeto_alternativo as indicador_trajeto_alternativo
                )
            ) as trip_info,
            trajetos_alternativos
        from deduplicado
        group by
            data,
            feed_version,
            feed_start_date,
            tipo_dia,
            tipo_os,
            servico,
            consorcio,
            sentido,
            extensao,
            partidas,
            quilometragem,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            modo,
            trajetos_alternativos
    )
select
    data,
    feed_version,
    feed_start_date,
    tipo_dia,
    tipo_os,
    servico,
    consorcio,
    sentido,
    extensao,
    partidas,
    quilometragem,
    faixa_horaria_inicio,
    faixa_horaria_fim,
    modo,
    trip_info,
    trajetos_alternativos,
    '' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from viagens_agrupadas