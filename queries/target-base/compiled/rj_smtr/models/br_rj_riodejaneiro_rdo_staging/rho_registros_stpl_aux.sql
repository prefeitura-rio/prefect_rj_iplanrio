

-- Tabela auxiliar para manter os dados com os mesmos identificadores:
-- data e hora de transacao, linha e operadora desagregados antes de somar na tabela final
-- Foi criada para não ter o risco de somar os dados do mesmo arquivo mais de uma vez






    
        

        

        
    



WITH rho_new AS (
    SELECT
        data_transacao,
        hora_transacao,
        data_particao AS data_arquivo_rho,
        linha AS servico_riocard,
        operadora,
        total_pagantes AS quantidade_transacao_pagante,
        total_gratuidades AS quantidade_transacao_gratuidade,
        timestamp_captura AS datetime_captura
    FROM
        `rj-smtr`.`br_rj_riodejaneiro_rdo_staging`.`rho_registros_stpl`
    
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

    

        UNION ALL

        SELECT
            *
        FROM
            `rj-smtr`.`br_rj_riodejaneiro_rdo_staging`.`rho_registros_stpl_aux`
        WHERE
            data_transacao IN ('2021-12-31', '2022-01-01', '2021-12-29', '2021-12-30', '2021-12-28', '2021-12-22', '2021-12-23', '2021-12-24', '2021-12-26', '2021-12-27')
    
)
SELECT
    data_transacao,
    hora_transacao,
    data_arquivo_rho,
    servico_riocard,
    operadora,
    quantidade_transacao_pagante,
    quantidade_transacao_gratuidade,
    datetime_captura
FROM
    (
        SELECT
            *,
            ROW_NUMBER() OVER(
                PARTITION BY
                    data_transacao,
                    hora_transacao,
                    data_arquivo_rho,
                    servico_riocard,
                    operadora
                ORDER BY
                    datetime_captura DESC
                ) AS rn
        FROM
            rho_complete_partitions
    )
WHERE
    rn = 1