




    
    


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
), os as (
        select * from __dbt__cte__aux_ordem_servico_diaria
    
    ),
    faixa as (
        select * from `rj-smtr`.`planejamento`.`ordem_servico_faixa_horaria`
    
    )
select
    f.feed_version,
    f.feed_start_date,
    f.tipo_os,
    f.servico,
    o.vista,
    f.consorcio,
    o.extensao_ida,
    o.extensao_volta,
    f.tipo_dia,
    o.horario_inicio,
    o.horario_fim,
    o.partidas_ida as partidas_ida_dia,
    o.partidas_volta as partidas_volta_dia,
    o.viagens_planejadas as viagens_dia,
    f.faixa_horaria_inicio,
    f.faixa_horaria_fim,
    safe_cast(null as int64) as partidas_ida,
    safe_cast(null as int64) as partidas_volta,
    f.partidas,
    f.quilometragem,
    f.versao_modelo
from faixa as f
left join os as o using (feed_start_date, tipo_os, tipo_dia, servico)
where feed_start_date in ()