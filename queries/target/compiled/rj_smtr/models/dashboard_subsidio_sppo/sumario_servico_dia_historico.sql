
    


with
    viagem_planejada as (
        (
            select distinct v.data, v.servico, o.vista
            from `rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada` as v
            -- `rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada` AS v
            left join
                `rj-smtr`.`projeto_subsidio_sppo`.`subsidio_data_versao_efetiva` as sdve
                -- rj-smtr.projeto_subsidio_sppo.subsidio_data_versao_efetiva AS sdve
                using (data)
            left join
                `rj-smtr`.`gtfs`.`ordem_servico` as o
                -- rj-smtr.gtfs.ordem_servico AS o
                on sdve.feed_start_date = o.feed_start_date
                and v.servico = o.servico
                and sdve.tipo_os = o.tipo_os
            where data >= "2024-05-01"
        )
        union all
        (
            select distinct `data`, servico, vista
            from `rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada`
            -- `rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada`
            where
                (id_tipo_trajeto = 0 or id_tipo_trajeto is null)
                and data < "2024-05-01"
        )
    ),
    -- v1: Valor do subsídio pré glosa por tipos de viagem (Antes de 2023-01-16)
    sumario_sem_glosa as (
        select
            `data`,
            tipo_dia,
            consorcio,
            servico,
            vista,
            viagens_subsidio as viagens,
            distancia_total_subsidio as km_apurada,
            distancia_total_planejada as km_planejada,
            perc_distancia_total_subsidio as perc_km_planejada,
            valor_total_subsidio as valor_subsidio_pago,
            null as valor_penalidade
        from `rj-smtr`.`dashboard_subsidio_sppo`.`sumario_dia`
        -- `rj-smtr`.`dashboard_subsidio_sppo`.`sumario_dia`
        left join viagem_planejada using (`data`, servico)
    ),
    -- v2: Valor do subsídio pós glosa por tipos de viagem (2023-01-16 a 2023-07-15 e
    -- após de 2023-09-01)
    sumario_com_glosa as (
        select
            `data`,
            tipo_dia,
            consorcio,
            servico,
            vista,
            viagens,
            km_apurada,
            km_planejada,
            perc_km_planejada,
            valor_subsidio_pago,
            valor_penalidade
        from `rj-smtr`.`dashboard_subsidio_sppo`.`sumario_servico_dia`
        -- `rj-smtr`.`dashboard_subsidio_sppo`.`sumario_servico_dia`
        left join viagem_planejada using (`data`, servico)
    ),
    -- Valor do subsídio sem glosas - Suspenso por Decisão Judicial (Entre 2023-07-16
    -- e 2023-08-31) (R$ 2.81/km em 2023)
    subsidio_total_glosa_suspensa as (
        select
            data,
            servico,
            case
                when perc_km_planejada >= 80
                then
                    round(
                        (
                            coalesce(km_apurada_autuado_ar_inoperante, 0)
                            + coalesce(km_apurada_autuado_seguranca, 0)
                            + coalesce(km_apurada_autuado_limpezaequipamento, 0)
                            + coalesce(km_apurada_licenciado_sem_ar_n_autuado, 0)
                            + coalesce(km_apurada_licenciado_com_ar_n_autuado, 0)
                        )
                        * 2.81,
                        2
                    )
                else 0
            end as valor_subsidio_pago,
            0 as valor_penalidade
        from `rj-smtr`.`dashboard_subsidio_sppo`.`sumario_servico_dia_tipo`
        -- `rj-smtr`.`dashboard_subsidio_sppo`.`sumario_servico_dia_tipo`
        where data between "2023-07-16" and "2023-08-31"
    ),
    -- v3: Sumário subsídio sem glosas - Suspenso por Decisão Judicial (Entre
    -- 2023-07-16 e 2023-08-31)
    sumario_glosa_suspensa as (
        select
            s.* except (valor_subsidio_pago, valor_penalidade),
            g.valor_subsidio_pago,
            g.valor_penalidade
        from subsidio_total_glosa_suspensa as g
        left join sumario_com_glosa as s using (`data`, servico)
    ),
    dados_completos as (
        select *
        from sumario_sem_glosa
        union all
        (
            select *
            from sumario_com_glosa
            where `data` < "2023-07-16" or `data` > "2023-08-31"
        )
        union all
        (select * from sumario_glosa_suspensa)
    )
select *, current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from dados_completos

    where data between date('2022-01-01T01:00:00') and date('2022-01-01T01:00:00')
