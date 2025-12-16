
        -- depends_on: `rj-smtr`.`monitoramento_staging`.`staging_gps_conecta`
        -- depends_on: __dbt__cte__aux_gps_filtrada
        -- depends_on: `rj-smtr`.`monitoramento`.`gps_onibus_conecta`
        
        
        
        
        
        
    

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
), data_hora as (
            select
                extract(date from timestamp_array) as data,
                extract(hour from timestamp_array) as hora,
            from
                unnest(
                    generate_timestamp_array(
                        "2022-01-01T00:00:00",
                        "2022-01-01T01:00:00",
                        interval 1 hour
                    )
                ) as timestamp_array
        ),
        gps_data as (
            select data, datetime_gps, latitude, longitude
            from `rj-smtr`.`monitoramento_staging`.`staging_gps_conecta`
            where
                data between date("2022-01-01T00:00:00") and date(
                    "2022-01-01T01:00:00"
                )
            qualify
                row_number() over (
                    partition by id_veiculo, datetime_gps, servico
                )
                = 1
        ),
        gps_raw as (
            select
                extract(date from datetime_gps) as data,
                extract(hour from datetime_gps) as hora,
                count(*) as q_gps_raw
            from gps_data
            group by 1, 2
        ),
        gps_filtrada as (
            select
                extract(date from datetime_gps) as data,
                extract(hour from datetime_gps) as hora,
                count(*) as q_gps_filtrada
            from
                -- `rj-smtr.br_rj_riodejaneiro_onibus_gps.sppo_aux_registros_filtrada`
                __dbt__cte__aux_gps_filtrada
            where
                data between date("2022-01-01T00:00:00") and date(
                    "2022-01-01T01:00:00"
                )
            group by 1, 2
        ),
        gps_sppo as (
            select
                data,
                extract(hour from datetime_gps) as hora,
                count(*) as q_gps_treated
            from
                -- `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo`
                `rj-smtr`.`monitoramento`.`gps_onibus_conecta`
            where
                data between date("2022-01-01T00:00:00") and date(
                    "2022-01-01T01:00:00"
                )
            group by 1, 2
        ),
        gps_join as (
            select
                *,
                safe_divide(q_gps_filtrada, q_gps_raw) as indice_tratamento_raw,
                safe_divide(
                    q_gps_treated, q_gps_filtrada
                ) as indice_tratamento_filtrada,
                case
                    when
                        q_gps_raw = 0
                        or q_gps_filtrada = 0
                        or q_gps_treated = 0  -- Hipótese de perda de dados no tratamento
                        or q_gps_raw is null
                        or q_gps_filtrada is null
                        or q_gps_treated is null  -- Hipótese de perda de dados no tratamento
                        or (q_gps_raw < q_gps_filtrada)
                        or (q_gps_filtrada < q_gps_treated)  -- Hipótese de duplicação de dados
                        or (coalesce(safe_divide(q_gps_filtrada, q_gps_raw), 0) < 0.96)  -- Hipótese de perda de dados no tratamento (superior a 3%)
                        or (
                            coalesce(safe_divide(q_gps_treated, q_gps_filtrada), 0)
                            < 0.96
                        )  -- Hipótese de perda de dados no tratamento (superior a 3%)
                    then false
                    else true
                end as status
            from data_hora
            left join gps_raw using (data, hora)
            left join gps_filtrada using (data, hora)
            left join gps_sppo using (data, hora)
        )
    select *
    from gps_join
    where status is false