select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      with
        viagem_planejada as (
            select distinct
                data,
                servico,
                faixa_horaria_inicio,
                round(distancia_total_planejada, 3) as distancia_total_planejada
            from `rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada`
            -- `rj-smtr.projeto_subsidio_sppo.viagem_planejada`
            where
                data between date("2022-01-01T00:00:00") and date(
                    "2022-01-01T01:00:00"
                )
        ),
        os_faixa as (
            select
                date(
                    datetime(data) + interval cast(
                        split(faixa_horaria_inicio, ":")[safe_offset(0)] as int64
                    ) hour
                ) as data,
                servico,
                datetime(data) + interval cast(
                    split(faixa_horaria_inicio, ":")[safe_offset(0)] as int64
                ) hour as faixa_horaria_inicio,
                round(sum(quilometragem), 3) as quilometragem
            from `rj-smtr`.`projeto_subsidio_sppo`.`subsidio_data_versao_efetiva`
            -- `rj-smtr.projeto_subsidio_sppo.subsidio_data_versao_efetiva`
            left join
                `rj-smtr`.`planejamento`.`ordem_servico_faixa_horaria`
                -- `rj-smtr.planejamento.ordem_servico_faixa_horaria`
                using (feed_start_date, tipo_os, tipo_dia)
            where
                data between date_sub(
                    date("2022-01-01T00:00:00"), interval 1 day
                ) and date("2022-01-01T01:00:00")
            group by 1, 2, 3
        )
        
    select *
    from viagem_planejada p
    full join
        (
            select
                * except (servico),
                -- alteração referente ao Processo.rio MTR-CAP-2025/06098
                case
                    when faixa_horaria_inicio = "2025-06-01T00:00:00"
                    then
                        case
                            when servico = "LECD108"
                            then "LECD112"
                            when servico = "864"
                            then "LECD122"
                            else servico
                        end
                    else servico
                end as servico,
            from os_faixa
            where quilometragem != 0
        ) using (data, servico, faixa_horaria_inicio)
    
    where
        quilometragem != distancia_total_planejada
        
      
    ) dbt_internal_test