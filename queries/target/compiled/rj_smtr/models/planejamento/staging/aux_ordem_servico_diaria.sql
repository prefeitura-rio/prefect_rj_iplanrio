

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