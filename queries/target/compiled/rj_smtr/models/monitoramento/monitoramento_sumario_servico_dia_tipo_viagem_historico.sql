

with __dbt__cte__monitoramento_servico_dia_tipo_viagem as (




select
    data,
    tipo_dia,
    consorcio,
    servico,
    tipo_viagem,
    indicador_ar_condicionado,
    viagens,
    km_apurada
from `rj-smtr`.`dashboard_subsidio_sppo`.`sumario_servico_tipo_viagem_dia`
-- `rj-smtr.dashboard_subsidio_sppo.sumario_servico_tipo_viagem_dia`
where
    data < date("2024-08-16")
    
),  __dbt__cte__monitoramento_servico_dia_tipo_viagem_v2 as (


select
    data,
    tipo_dia,
    consorcio,
    servico,
    tipo_viagem,
    indicador_ar_condicionado,
    sum(viagens_faixa) as viagens,
    sum(km_apurada_faixa) as km_apurada
from `rj-smtr`.`financeiro`.`subsidio_faixa_servico_dia_tipo_viagem`
-- `rj-smtr.financeiro.subsidio_faixa_servico_dia_tipo_viagem`
where
    data >= date("2024-08-16")
    and tipo_viagem != "Sem viagem apurada"
    
group by data, tipo_dia, consorcio, servico, tipo_viagem, indicador_ar_condicionado
) select *
from __dbt__cte__monitoramento_servico_dia_tipo_viagem
where
    data < date("2024-08-16")
union all

select *
from __dbt__cte__monitoramento_servico_dia_tipo_viagem_v2
where
    data >= date("2024-08-16")
    and tipo_viagem != "Sem viagem apurada"