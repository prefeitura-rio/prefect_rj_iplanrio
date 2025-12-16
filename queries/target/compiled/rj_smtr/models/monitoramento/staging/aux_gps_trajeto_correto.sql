

with
     __dbt__cte__aux_gps_realocacao as (




-- 1. Filtra realocações válidas dentro do intervalo de GPS avaliado
with
    realocacao as (
        select
            * except (datetime_saida),
            case
                when datetime_saida is null then datetime_operacao else datetime_saida
            end as datetime_saida
        from `rj-smtr`.`monitoramento_staging`.`staging_realocacao_conecta`
        where
            
    (
    
    
    

    
        data = date("2022-01-01T00:00:00")
        and hora between 0 and 1

    

)

            -- Realocação deve acontecer após o registro de GPS e até 1 hora depois
            and datetime_diff(datetime_operacao, datetime_entrada, minute)
            between 0 and 60
            and (
                datetime_saida >= datetime("2022-01-01T00:00:00")
                or datetime_operacao >= datetime("2022-01-01T00:00:00")
            )
    ),
    -- 2. Altera registros de GPS com servicos realocados
    gps as (
        select *
        from `rj-smtr`.`monitoramento_staging`.`staging_gps_conecta`
        where
            
    (
    
    
    

    
        data = date("2022-01-01T00:00:00")
        and hora between 0 and 1

    

)

            and datetime_gps > "2022-01-01T00:00:00"
            and datetime_gps <= "2022-01-01T01:00:00"
    ),
    servicos_realocados as (
        select
            g.* except (servico),
            g.servico as servico_gps,
            r.servico as servico_realocado,
            r.datetime_operacao as datetime_realocado
        from gps g
        inner join
            realocacao r
            on g.id_veiculo = r.id_veiculo
            and g.servico != r.servico
            and g.datetime_gps between r.datetime_entrada and r.datetime_saida
    ),
    gps_com_realocacao as (
        select
            g.* except (servico),
            coalesce(s.servico_realocado, g.servico) as servico,
            s.datetime_realocado
        from gps g
        left join servicos_realocados s using (id_veiculo, datetime_gps)
    )
-- Filtra realocacao mais recente para cada timestamp
select * except (rn)
from
    (
        select
            *,
            row_number() over (
                partition by id_veiculo, datetime_gps
                order by datetime_captura desc, datetime_realocado desc
            ) as rn
        from gps_com_realocacao
    )
where rn = 1
),  __dbt__cte__aux_gps_filtrada as (


with
    box as (
        /* Geometria de caixa que contém a área do município de Rio de Janeiro.*/
        select min_longitude, min_latitude, max_longitude, max_latitude
        from rj-smtr.br_rj_riodejaneiro_geo.limites_geograficos_caixa
    ),
    gps as (
        select *, st_geogpoint(longitude, latitude) posicao_veiculo_geo
        from __dbt__cte__aux_gps_realocacao
    ),
    filtrada as (
        select
            data,
            hora,
            datetime_gps,
            id_veiculo,
            servico,
            latitude,
            longitude,
            posicao_veiculo_geo,
            datetime_captura,
            velocidade,
        from gps
        cross join box
        where
            st_intersectsbox(
                posicao_veiculo_geo,
                min_longitude,
                min_latitude,
                max_longitude,
                max_latitude
            )
    )
select *
from filtrada
), registros as (
        select id_veiculo, servico, data, posicao_veiculo_geo, datetime_gps
        from __dbt__cte__aux_gps_filtrada
    ),
    servico_planejado as (
        select data, feed_start_date, servico, sentido, trip_info, trajetos_alternativos
        from `rj-smtr`.`planejamento`.`servico_planejado_faixa_horaria`
        
        where
            data between date('2022-01-01T00:00:00') and date(
                '2022-01-01T01:00:00'
            )
    ),
    servico_planejado_trip as (
        select sp.data, sp.feed_start_date, sp.servico, sp.sentido, trip.shape_id
        from servico_planejado sp, unnest(sp.trip_info) as trip
        where trip.shape_id is not null
    ),
    servico_planejado_trajetos_alternativos as (
        select
            sp.data,
            sp.feed_start_date,
            sp.servico,
            sp.sentido,
            trajetos_alternativos.shape_id
        from
            servico_planejado sp,
            unnest(sp.trajetos_alternativos) as trajetos_alternativos
        where trajetos_alternativos.shape_id is not null
    ),
    shape_union as (
        select distinct data, feed_start_date, servico, sentido, shape_id
        from
            (
                select data, feed_start_date, servico, sentido, shape_id
                from servico_planejado_trip
                union all
                select data, feed_start_date, servico, sentido, shape_id
                from servico_planejado_trajetos_alternativos
            )
    ),
    intersec as (
        select
            r.*,
            s.shape_id,
            st_dwithin(
                sg.shape, r.posicao_veiculo_geo, 30
            ) as indicador_intersecao
        from registros r
        left join shape_union s using (servico, data)
        left join
            `rj-smtr`.`gtfs`.`shapes_geom` sg using (
                
                feed_start_date, shape_id
            )
    ),
    indicador as (
        select
            *,
            count(case when indicador_intersecao then 1 end) over (
                partition by id_veiculo
                order by
                    unix_seconds(timestamp(datetime_gps))
                    range
                    between 600 preceding
                    and current row
            )
            >= 1 as indicador_trajeto_correto
        from intersec
    )
select
    data,
    datetime_gps,
    id_veiculo,
    servico,
    logical_or(indicador_trajeto_correto) as indicador_trajeto_correto
from indicador
group by id_veiculo, servico, data, datetime_gps