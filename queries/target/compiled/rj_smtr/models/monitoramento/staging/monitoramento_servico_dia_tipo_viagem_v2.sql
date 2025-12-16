

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