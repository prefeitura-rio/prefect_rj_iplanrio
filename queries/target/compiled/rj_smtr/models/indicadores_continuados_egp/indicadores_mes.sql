


SELECT
    ano,
    mes,
    modo AS modo,
    "Passageiros pagantes por mês" AS nome_indicador,
    quantidade_passageiro_pagante_mes AS valor,
    data_ultima_atualizacao
FROM `rj-smtr`.`indicadores_continuados_egp_staging`.`passageiro_pagante`
UNION ALL

SELECT
    ano,
    mes,
    modo AS modo,
    "Gratuidades por mês" AS nome_indicador,
    quantidade_passageiro_gratuidade_mes AS valor,
    data_ultima_atualizacao
FROM `rj-smtr`.`indicadores_continuados_egp_staging`.`passageiro_gratuidade`
UNION ALL

SELECT
    ano,
    mes,
    modo AS modo,
    "Frota operante por mês" AS nome_indicador,
    quantidade_veiculo_mes AS valor,
    data_ultima_atualizacao
FROM `rj-smtr`.`indicadores_continuados_egp_staging`.`frota_operante`
UNION ALL

SELECT
    ano,
    mes,
    modo AS modo,
    "Idade média da frota operante por mês" AS nome_indicador,
    idade_media_veiculo_mes AS valor,
    data_ultima_atualizacao
FROM `rj-smtr`.`indicadores_continuados_egp_staging`.`idade_media_frota_operante_onibus`

