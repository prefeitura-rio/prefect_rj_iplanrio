



    
        

        

        
    


WITH rho_new AS (
    SELECT
        data_transacao,
        hora_transacao,
        data_processamento,
        data_particao AS data_arquivo_rho,
        linha AS servico_riocard,
        linha_rcti AS linha_riocard,
        operadora,
        total_pagantes_cartao AS quantidade_transacao_cartao,
        total_pagantes_especie AS quantidade_transacao_especie,
        total_gratuidades AS quantidade_transacao_gratuidade,
        registro_processado,
        timestamp_captura AS datetime_captura
    FROM
        `rj-smtr`.`br_rj_riodejaneiro_rdo_staging`.`rho_registros_sppo`
    
        WHERE
            ano BETWEEN
                EXTRACT(YEAR FROM DATE("2022-01-01T00:00:00"))
                AND EXTRACT(YEAR FROM DATE("2022-01-01T01:00:00"))
            AND mes BETWEEN
                EXTRACT(MONTH FROM DATE("2022-01-01T00:00:00"))
                AND EXTRACT(MONTH FROM DATE("2022-01-01T01:00:00"))
            AND dia BETWEEN
                EXTRACT(DAY FROM DATE("2022-01-01T00:00:00"))
                AND EXTRACT(DAY FROM DATE("2022-01-01T01:00:00"))
    
),
rho_complete_partitions AS (
    SELECT
        *
    FROM
        rho_new

    
),
-- Deduplica os dados com base na data e hora da transacao, linha, linha_rcti e operadora
rho_rn AS (
    SELECT
        *,
        ROW_NUMBER() OVER(
            PARTITION BY
                data_transacao,
                hora_transacao,
                data_arquivo_rho,
                servico_riocard,
                linha_riocard,
                operadora
            ORDER BY
                datetime_captura DESC
        ) AS rn
    FROM
        rho_complete_partitions
)
SELECT
    * EXCEPT(rn)
FROM
    rho_rn
WHERE
    rn = 1