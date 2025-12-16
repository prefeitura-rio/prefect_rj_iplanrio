






    
    


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
), terminais as (
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