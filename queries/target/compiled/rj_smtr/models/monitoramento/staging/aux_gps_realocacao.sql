



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