

SELECT
    modo,
    EXTRACT(DATE FROM datetime_gps) AS data,
    EXTRACT(HOUR FROM datetime_gps) AS hora,
    datetime_gps,
    datetime_captura,
    id_operadora,
    operadora,
    id_servico_jae,
    -- s.servico,
    servico_jae,
    descricao_servico_jae,
    id_veiculo,
    id_validador,
    id_transmissao_gps,
    latitude,
    longitude,
    sentido,
    estado_equipamento,
    temperatura,
    versao_app,
    '' as versao
FROM
(
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY id_transmissao_gps ORDER BY datetime_captura DESC) AS rn
    FROM
        `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`gps_validador_aux`
    
        WHERE
            DATE(data) BETWEEN DATE("2022-01-01T00:00:00") AND DATE("2022-01-01T01:00:00")
            AND datetime_captura > DATETIME("2022-01-01T00:00:00") AND datetime_captura <= DATETIME("2022-01-01T01:00:00")
    
)
WHERE
    rn = 1
    AND modo = "Van"