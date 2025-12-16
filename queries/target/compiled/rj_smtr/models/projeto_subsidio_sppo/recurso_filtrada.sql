

SELECT
    data,
    hora,
    SAFE_CAST(DATETIME(TIMESTAMP(timestamp_captura), "America/Sao_Paulo") AS DATETIME) as timestamp_captura,
    safe_cast(modo as string) as modo,
    safe_cast(id_recurso as string) as id_recurso,
    safe_cast(split(data_recurso,".")[OFFSET(0)] as datetime) as data_recurso,
    safe_cast(protocolo as string) as protocolo,
    safe_cast(data_viagem as date) as data_viagem,
    safe_cast(safe_cast(hora_partida as timestamp) as time) as hora_partida,
    safe_cast(safe_cast(hora_chegada as timestamp) as time) as hora_chegada,
    safe_cast(id_veiculo as string) as id_veiculo,
    safe_cast(servico as string) as servico,
    REGEXP_EXTRACT(servico, r'[0-9]+') as linha,
    concat("S", left(safe_cast(tipo_servico as string), 1)) as tipo_servico,
    REGEXP_EXTRACT(split(servico, "-")[OFFSET(0)], r'[A-Z]+') as tipo_servico_extra,
    left(safe_cast(sentido as string),1) as sentido,
    safe_cast(status as string) as status,
    null as id_julgamento
FROM rj-smtr-staging.projeto_subsidio_sppo_staging.recurso

where
    safe_cast(data_viagem as date) between date('2022-07-01 00:00:00') and date('2022-07-15 00:00:00')
    and SAFE_CAST(DATETIME(TIMESTAMP(timestamp_captura), "America/Sao_Paulo" ) AS DATETIME) = '2022-11-04T14:17:00'
