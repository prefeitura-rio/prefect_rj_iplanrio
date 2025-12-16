select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      WITH
sumario_servico_dia_pagamento AS (
    SELECT
        *,
    FROM
        
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`dashboard_subsidio_sppo_v2`.`sumario_servico_dia_pagamento` where data between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00') and data < date('2025-01-05'))
    WHERE
        DATA BETWEEN DATE("2022-01-01T00:00:00")
        AND DATE("2022-01-01T01:00:00")),
subsidio_valor_km_tipo_viagem AS (
    SELECT
        data_inicio,
        data_fim,
        MAX(subsidio_km) AS subsidio_km_teto
    FROM
        -- `rj-smtr`.`dashboard_subsidio_sppo_staging`.`subsidio_valor_km_tipo_viagem`
        `rj-smtr`.`dashboard_subsidio_sppo_staging`.`subsidio_valor_km_tipo_viagem`
    WHERE
        subsidio_km > 0
    GROUP BY
        1,
        2)
SELECT
    *
FROM
    sumario_servico_dia_pagamento AS s
LEFT JOIN
    subsidio_valor_km_tipo_viagem AS p
ON
    s.data BETWEEN p.data_inicio
    AND p.data_fim
WHERE
    NOT(ROUND((valor_a_pagar - valor_penalidade)/subsidio_km_teto,2) <= ROUND(km_apurada_dia+0.01,2))
      
    ) dbt_internal_test