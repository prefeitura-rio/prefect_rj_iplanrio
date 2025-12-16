



    

    


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
),  __dbt__cte__aux_gps_velocidade as (


with
    anterior as (
        select
            data,
            hora,
            id_veiculo,
            datetime_gps,
            servico,
            latitude,
            longitude,
            posicao_veiculo_geo,
            datetime_captura,
            velocidade as velocidade_original,
            lag(posicao_veiculo_geo) over w as posicao_anterior,
            lag(datetime_gps) over w as datetime_anterior
        from __dbt__cte__aux_gps_filtrada
        window w as (partition by id_veiculo, servico order by datetime_gps)
    ),
    calculo_velocidade as (
        select
            *,
            st_distance(posicao_veiculo_geo, posicao_anterior) as distancia,
            ifnull(
                safe_divide(
                    st_distance(posicao_veiculo_geo, posicao_anterior),
                    datetime_diff(datetime_gps, datetime_anterior, second)
                ),
                0
            )
            * 3.6 as velocidade
        from anterior
    ),
    media as (
        select
            data,
            datetime_gps,
            id_veiculo,
            servico,
            distancia,
            velocidade,
            -- Calcula média móvel
            avg(velocidade) over (
                partition by id_veiculo, servico
                order by
                    unix_seconds(timestamp(datetime_gps))
                    range
                    between 600 preceding
                    and current row
            ) as velocidade_media
        from calculo_velocidade
    )
select
    data,
    datetime_gps,
    id_veiculo,
    servico,
    distancia,
    round(
        case
            when velocidade_media > 60
            then 60
            else velocidade_media
        end,
        1
    ) as velocidade,
    case
        when velocidade_media < 3
        then false
        else true
    end as indicador_em_movimento
from media
),  __dbt__cte__aux_gps_parada as (







    
    


with
    terminais as (
        select
            st_geogpoint(stop_lon, stop_lat) as ponto_parada,
            stop_name as nome_parada,
            'terminal' as tipo_parada
        from `rj-smtr`.`gtfs`.`stops`
        
        where location_type = "1" and feed_start_date in ()
    ),
    garagens as (
        select
            * except (geometry_wkt, operador),
            st_geogfromtext(geometry_wkt, make_valid => true) as geometry,
            operador as nome_parada,
            'garagem' as tipo_parada
        from `rj-smtr`.`monitoramento_staging`.`garagens`
        where
            ativa
            and inicio_vigencia <= date('2022-01-01T01:00:00')
            and (
                fim_vigencia is null
                or fim_vigencia >= date('2022-01-01T00:00:00')
            )
    ),
    posicoes_veiculos as (
        select id_veiculo, datetime_gps, data, servico, posicao_veiculo_geo
        from __dbt__cte__aux_gps_filtrada
    ),
    veiculos_em_garagens as (
        select
            v.id_veiculo,
            v.datetime_gps,
            v.data,
            v.servico,
            v.posicao_veiculo_geo,
            g.nome_parada,
            g.tipo_parada
        from posicoes_veiculos v
        join
            garagens g
            on st_intersects(v.posicao_veiculo_geo, g.geometry)
            and v.data >= g.inicio_vigencia
            and (g.fim_vigencia is null or v.data <= g.fim_vigencia)
    ),
    terminais_proximos as (
        select
            v.id_veiculo,
            v.datetime_gps,
            v.data,
            v.servico,
            v.posicao_veiculo_geo,
            t.nome_parada,
            t.tipo_parada,
            round(
                st_distance(v.posicao_veiculo_geo, t.ponto_parada), 1
            ) as distancia_parada,
            case
                when
                    round(st_distance(v.posicao_veiculo_geo, t.ponto_parada), 1)
                    < 250
                then t.tipo_parada
                else null
            end as status_terminal
        from posicoes_veiculos v
        join
            terminais t
            on st_dwithin(
                v.posicao_veiculo_geo,
                t.ponto_parada,
                250
            )
        qualify
            row_number() over (
                partition by v.datetime_gps, v.id_veiculo, v.servico
                order by st_distance(v.posicao_veiculo_geo, t.ponto_parada)
            )
            = 1
    ),
    resultados_combinados as (
        select
            v.id_veiculo,
            v.datetime_gps,
            v.data,
            v.servico,
            coalesce(g.tipo_parada, t.status_terminal) as tipo_parada,
            coalesce(g.nome_parada, t.nome_parada) as nome_parada
        from posicoes_veiculos v
        left join
            veiculos_em_garagens g
            on v.id_veiculo = g.id_veiculo
            and v.datetime_gps = g.datetime_gps
        left join
            terminais_proximos t
            on v.id_veiculo = t.id_veiculo
            and v.datetime_gps = t.datetime_gps
    )
select data, datetime_gps, id_veiculo, servico, tipo_parada, nome_parada
from resultados_combinados
),  __dbt__cte__aux_gps_trajeto_correto as (


with
    registros as (
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
), registros as (
        select
            data,
            hora,
            id_veiculo,
            datetime_gps,
            datetime_captura,
            velocidade,
            servico,
            latitude,
            longitude,
        from __dbt__cte__aux_gps_filtrada
    ),
    velocidades as (
        select
            id_veiculo,
            datetime_gps,
            servico,
            velocidade,
            distancia,
            indicador_em_movimento
        from __dbt__cte__aux_gps_velocidade
    ),
    paradas as (
        select id_veiculo, datetime_gps, servico, tipo_parada
        from __dbt__cte__aux_gps_parada
    ),
    indicadores as (
        select id_veiculo, datetime_gps, servico, indicador_trajeto_correto
        from __dbt__cte__aux_gps_trajeto_correto
    ),
    novos_dados as (
        select
            date(r.datetime_gps) as data,
            extract(hour from r.datetime_gps) as hora,
            r.datetime_gps,
            r.id_veiculo,
            r.servico,
            r.latitude,
            r.longitude,
            case
                when
                    indicador_em_movimento is true and indicador_trajeto_correto is true
                then 'Em operação'
                when
                    indicador_em_movimento is true
                    and indicador_trajeto_correto is false
                then 'Operando fora trajeto'
                when indicador_em_movimento is false
                then
                    case
                        when tipo_parada is not null
                        then concat("Parado ", tipo_parada)
                        when indicador_trajeto_correto is true
                        then 'Parado trajeto correto'
                        else 'Parado fora trajeto'
                    end
            end as status,
            r.velocidade as velocidade_instantanea,
            v.velocidade as velocidade_estimada_10_min,
            v.distancia,
            0 as priority
        from registros r
        join indicadores i using (id_veiculo, datetime_gps, servico)
        join velocidades v using (id_veiculo, datetime_gps, servico)
        join paradas p using (id_veiculo, datetime_gps, servico)
    ),
    particoes_completas as (
        select *
        from novos_dados

        
    )
select
    * except (priority),
    '' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from particoes_completas
qualify
    row_number() over (partition by id_veiculo, datetime_gps, servico order by priority)
    = 1