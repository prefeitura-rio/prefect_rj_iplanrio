





WITH gratuidade_complete_partitions AS (
    SELECT
        CAST(CAST(cd_cliente AS FLOAT64) AS INT64) AS id_cliente,
        id AS id_gratuidade,
        tipo_gratuidade,
        data_inclusao AS data_inicio_validade,
        timestamp_captura
    FROM
        `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`gratuidade`
),
gratuidade_deduplicada AS (
    SELECT
        * EXCEPT(rn)
    FROM
        (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY id_gratuidade ORDER BY timestamp_captura DESC) AS rn
            FROM
                gratuidade_complete_partitions
        )
    WHERE
        rn = 1
)
SELECT
    id_cliente,
    id_gratuidade,
    tipo_gratuidade,
    data_inicio_validade,
    LEAD(data_inicio_validade) OVER (PARTITION BY id_cliente ORDER BY data_inicio_validade) AS data_fim_validade,
    timestamp_captura
FROM
    gratuidade_deduplicada