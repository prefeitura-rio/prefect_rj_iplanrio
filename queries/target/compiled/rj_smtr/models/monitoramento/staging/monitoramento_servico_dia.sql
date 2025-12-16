


select
    data,
    tipo_dia,
    consorcio,
    servico,
    vista,
    viagens,
    km_apurada,
    km_planejada,
    perc_km_planejada,
    valor_subsidio_pago + coalesce(valor_penalidade, 0) as valor_subsidio_pago,
    coalesce(valor_penalidade, 0) as valor_penalidade
from `rj-smtr`.`dashboard_subsidio_sppo`.`sumario_servico_dia_historico`
    -- `rj-smtr.dashboard_subsidio_sppo.sumario_servico_dia_historico`
where
    data < date("2024-08-16")  -- noqa
    