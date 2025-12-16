

-- Tabela auxiliar para manter os dados com os mesmos identificadores:
-- data e hora de transacao, linha e operadora desagregados antes de somar na tabela
-- final
-- Foi criada para não ter o risco de somar os dados do mesmo arquivo mais de uma vez





    
        

        

        
    



with
    rho_new as (
        select
            data_transacao,
            hora_transacao,
            data_particao as data_arquivo_rho,
            trim(linha) as servico_riocard,
            trim(operadora) as operadora,
            total_pagantes as quantidade_transacao_pagante,
            total_gratuidades as quantidade_transacao_gratuidade,
            timestamp_captura as datetime_captura
        from `rj-smtr`.`br_rj_riodejaneiro_rdo_staging`.`rho_registros_stpl`
         where 
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
    rho_complete_partitions as (
        select *
        from rho_new

        

            union all

            select *
            from `rj-smtr`.`br_rj_riodejaneiro_rdo_staging`.`rho_registros_stpl_aux`
            where data_transacao in ('2021-12-26', '2021-12-27', '2021-12-25', '2021-12-24', '2021-12-23', '2021-12-17', '2021-12-18', '2021-12-19', '2021-12-20', '2021-12-21', '2021-12-22', '2021-12-14', '2002-12-31', '2021-11-18', '2021-12-15', '2021-12-13', '2021-12-16')
        
    )
select
    data_transacao,
    hora_transacao,
    data_arquivo_rho,
    servico_riocard,
    operadora,
    quantidade_transacao_pagante,
    quantidade_transacao_gratuidade,
    datetime_captura
from
    (
        select
            *,
            row_number() over (
                partition by
                    data_transacao,
                    hora_transacao,
                    data_arquivo_rho,
                    servico_riocard,
                    operadora
                order by datetime_captura desc
            ) as rn
        from rho_complete_partitions
    )
where rn = 1