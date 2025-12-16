

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
), anterior as (
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