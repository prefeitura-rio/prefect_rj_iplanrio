

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
), feed_start_date as (
        select
            feed_start_date,
            feed_start_date as data_inicio,
            coalesce(
                date_sub(
                    lead(feed_start_date) over (order by feed_start_date),
                    interval 1 day
                ),
                last_day(feed_start_date, month)
            ) as data_fim
        from
            (select distinct feed_start_date from __dbt__cte__aux_ordem_servico_diaria)
    ),
    ordem_servico_pivot as (
        select *
        from
            __dbt__cte__aux_ordem_servico_diaria pivot (
                max(partidas_ida) as partidas_ida,
                max(partidas_volta) as partidas_volta,
                max(viagens_planejadas) as viagens_planejadas,
                max(distancia_total_planejada) as km for
                tipo_dia in (
                    'Dia Útil' as du,
                    'Ponto Facultativo' as pf,
                    'Sabado' as sab,
                    'Domingo' as dom
                )
            )
    ),
    subsidio_feed_start_date_efetiva as (
        select
            data, split(tipo_dia, " - ")[0] as tipo_dia, tipo_dia as tipo_dia_original
        from `rj-smtr`.`projeto_subsidio_sppo`.`subsidio_data_versao_efetiva`
    )
select
    data,
    tipo_dia_original as tipo_dia,
    servico,
    vista,
    consorcio,
    sentido,
    case
        
        
                when
                    sentido
                     in ('I', 'C')
                     and tipo_dia = "Dia Útil"
                then
                     partidas_ida_du
                    
            
                when
                    sentido
                     in ('I', 'C')
                     and tipo_dia = "Ponto Facultativo"
                then
                     partidas_ida_pf
                    
            
                when
                    sentido
                     in ('I', 'C')
                     and tipo_dia = "Sabado"
                then
                    
                        round(
                            safe_divide(
                                (partidas_ida_du * km_sab), km_du
                            )
                        )
                    
            
                when
                    sentido
                     in ('I', 'C')
                     and tipo_dia = "Domingo"
                then
                    
                        round(
                            safe_divide(
                                (partidas_ida_du * km_dom), km_du
                            )
                        )
                    
            
                when
                    sentido
                     = "V"
                     and tipo_dia = "Dia Útil"
                then
                     partidas_volta_du
                    
            
                when
                    sentido
                     = "V"
                     and tipo_dia = "Ponto Facultativo"
                then
                     partidas_volta_pf
                    
            
                when
                    sentido
                     = "V"
                     and tipo_dia = "Sabado"
                then
                    
                        round(
                            safe_divide(
                                (partidas_volta_du * km_sab), km_du
                            )
                        )
                    
            
                when
                    sentido
                     = "V"
                     and tipo_dia = "Domingo"
                then
                    
                        round(
                            safe_divide(
                                (partidas_volta_du * km_dom), km_du
                            )
                        )
                    
            end as viagens_planejadas,
    horario_inicio as inicio_periodo,
    horario_fim as fim_periodo
from
    unnest(
        generate_date_array(
            (select min(data_inicio) from feed_start_date),
            (select max(data_fim) from feed_start_date)
        )
    ) as data
left join feed_start_date as d on data between d.data_inicio and d.data_fim
left join subsidio_feed_start_date_efetiva as sd using (data)
left join ordem_servico_pivot as o using (feed_start_date)
left join `rj-smtr`.`dashboard_operacao_onibus_staging`.`servicos_sentido` using (feed_start_date, servico)
where data < "2023-04-01"