



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
    