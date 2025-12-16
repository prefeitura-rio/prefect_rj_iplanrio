


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
), feed_info as (select * from `rj-smtr`.`gtfs`.`feed_info`),
    ordem_servico_pivot as (
        select *
        from
            __dbt__cte__aux_ordem_servico_diaria pivot (
                max(partidas_ida) as partidas_ida,
                max(partidas_volta) as partidas_volta,
                max(viagens_planejadas) as viagens_planejadas,
                max(distancia_total_planejada) as km for tipo_dia in (
                    'Dia Útil' as du,
                    'Ponto Facultativo' as pf,
                    'Sabado' as sab,
                    'Domingo' as dom
                )
            )
    ),
    ordem_servico_regular as (
        select feed_start_date, servico, partidas_ida_du, partidas_volta_du, km_du
        from ordem_servico_pivot
        where tipo_os = "Regular"
    ),
    ordem_servico_tratada as (
        select

            osp.* except (partidas_ida_du, partidas_volta_du, km_du),
            case
                when osp.partidas_ida_du = 0 or osp.partidas_ida_du is null
                then osr.partidas_ida_du
                else osp.partidas_ida_du
            end as partidas_ida_du,
            case
                when osp.partidas_volta_du = 0 or osp.partidas_volta_du is null
                then osr.partidas_volta_du
                else osp.partidas_volta_du
            end as partidas_volta_du,
            case
                when osp.km_du = 0 or osp.km_du is null then osr.km_du else osp.km_du
            end as km_du
        from ordem_servico_pivot osp
        left join ordem_servico_regular osr using (feed_start_date, servico)
    ),
    subsidio_feed_start_date_efetiva as (
        select
            data,
            tipo_os,
            split(tipo_dia, " - ")[0] as tipo_dia,
            tipo_dia as tipo_dia_original
        from `rj-smtr`.`projeto_subsidio_sppo`.`subsidio_data_versao_efetiva`
    ),
    ordem_servico_trips_shapes as (
        select distinct feed_start_date, servico, tipo_os, sentido
        from `rj-smtr`.`gtfs`.`ordem_servico_trips_shapes`
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
            (select min(feed_start_date) from feed_info),
            (select max(feed_end_date) from feed_info)
        )
    ) as data
left join feed_info as d on data between d.feed_start_date and d.feed_end_date
left join ordem_servico_tratada as o using (feed_start_date)
inner join subsidio_feed_start_date_efetiva as sd using (data, tipo_os)
left join ordem_servico_trips_shapes using (feed_start_date, servico, tipo_os)