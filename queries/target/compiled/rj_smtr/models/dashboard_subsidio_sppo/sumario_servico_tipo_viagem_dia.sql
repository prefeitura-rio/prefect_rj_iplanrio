
with
     __dbt__cte__aux_veiculo_dia_consolidada as (


select data, id_veiculo, placa, modo, tecnologia, status, indicadores
from `rj-smtr`.`veiculo`.`sppo_veiculo_dia`

union all

select data, id_veiculo, placa, modo, tecnologia, status, indicadores
from `rj-smtr`.`monitoramento`.`veiculo_dia`
where modo is null or (modo = 'ONIBUS' and tipo_veiculo not like '%ROD%')
), planejado as (
        select distinct `data`, tipo_dia, consorcio, servico
        from `rj-smtr`.`dashboard_subsidio_sppo`.`sumario_servico_dia_historico`
        where `data` <= date("2022-01-01T01:00:00")
    ),
    sumario_v1 as (  -- Viagens v1
        select
            `data`,
            servico,
            "Não classificado" as tipo_viagem,
            null as indicador_ar_condicionado,
            viagens,
            km_apurada
        from `rj-smtr`.`dashboard_subsidio_sppo`.`sumario_servico_dia_historico`
        where `data` < date("2023-01-16")
    ),
    tipo_viagem_v2 as (  -- Classifica os tipos de viagem (v2)
        select
            `data`,
            id_veiculo,
            status,
            safe_cast(
                json_value(indicadores, "$.indicador_ar_condicionado") as bool
            ) as indicador_ar_condicionado
        from __dbt__cte__aux_veiculo_dia_consolidada
        where
            `data` between date("2023-01-16") and date(
                "2022-01-01T01:00:00"
            )
    ),
    viagem_v2 as (
        select
            `data`,
            servico_realizado as servico,
            id_veiculo,
            id_viagem,
            distancia_planejada
        from `rj-smtr`.`projeto_subsidio_sppo`.`viagem_completa`  -- `rj-smtr`.`projeto_subsidio_sppo`.`viagem_completa`
        where
            `data` between date("2023-01-16") and date(
                "2022-01-01T01:00:00"
            )
    ),
    tipo_viagem_v2_atualizado as (
        select
            * except (status),
            case
                when status = "Nao licenciado"
                then "Não licenciado"
                when status = "Licenciado com ar e autuado (023.II)"
                then "Autuado por ar inoperante"
                when status = "Licenciado sem ar"
                then "Licenciado sem ar e não autuado"
                when status = "Licenciado com ar e não autuado (023.II)"
                then "Licenciado com ar e não autuado"
                else status
            end as status
        from tipo_viagem_v2
    ),
    sumario_v2 as (
        select
            v.`data`,
            v.servico,
            ve.status as tipo_viagem,
            ve.indicador_ar_condicionado,
            count(id_viagem) as viagens,
            round(sum(distancia_planejada), 2) as km_apurada
        from viagem_v2 v
        left join
            tipo_viagem_v2_atualizado ve
            on ve.`data` = v.`data`
            and ve.id_veiculo = v.id_veiculo
        group by 1, 2, 3, 4
    )
    (
        select
            v1.`data`,
            p.tipo_dia,
            p.consorcio,
            v1.servico,
            coalesce(v1.tipo_viagem, "Sem viagem apurada") as tipo_viagem,
            safe_cast(indicador_ar_condicionado as bool) as indicador_ar_condicionado,
            coalesce(v1.viagens, 0) as viagens,
            coalesce(v1.km_apurada, 0) as km_apurada
        from sumario_v1 v1
        inner join planejado p on p.`data` = v1.`data` and p.servico = v1.servico
        where p.`data` < date("2023-01-16")
    )
union all
(
    select
        v2.`data`,
        p.tipo_dia,
        p.consorcio,
        v2.servico,
        coalesce(v2.tipo_viagem, "Sem viagem apurada") as tipo_viagem,
        v2.indicador_ar_condicionado,
        coalesce(v2.viagens, 0) as viagens,
        coalesce(v2.km_apurada, 0) as km_apurada
    from sumario_v2 v2
    inner join planejado p on p.`data` = v2.`data` and p.servico = v2.servico
    where p.`data` >= date("2023-01-16")
)