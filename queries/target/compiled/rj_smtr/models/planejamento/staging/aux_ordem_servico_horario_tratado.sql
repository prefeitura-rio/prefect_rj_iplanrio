

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
), ordem_servico as (
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