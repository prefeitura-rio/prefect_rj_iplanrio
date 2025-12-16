



    
        

        

        
    


SELECT
    data_transacao,
    hora_transacao,
    servico_riocard,
    operadora,
    SUM(quantidade_transacao_pagante) AS quantidade_transacao_pagante,
    SUM(quantidade_transacao_gratuidade) AS quantidade_transacao_gratuidade
FROM
    `rj-smtr`.`br_rj_riodejaneiro_rdo_staging`.`rho_registros_stpl_aux`

    WHERE
        data_transacao
        
            IN ('2021-12-31', '2022-01-01', '2021-12-29', '2021-12-30', '2021-12-28', '2021-12-22', '2021-12-23', '2021-12-24', '2021-12-26', '2021-12-27')
        

GROUP BY
    1,
    2,
    3,
    4