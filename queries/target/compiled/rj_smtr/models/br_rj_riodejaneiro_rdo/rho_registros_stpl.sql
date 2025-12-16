



    
        

        

        
    


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
        
            IN ('2021-12-26', '2021-12-27', '2021-12-25', '2021-12-24', '2021-12-23', '2021-12-17', '2021-12-18', '2021-12-19', '2021-12-20', '2021-12-21', '2021-12-22', '2021-12-14', '2002-12-31', '2021-11-18', '2021-12-15', '2021-12-13', '2021-12-16')
        

GROUP BY
    1,
    2,
    3,
    4