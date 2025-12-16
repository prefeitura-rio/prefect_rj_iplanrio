WITH
sumario_servico_dia_tipo_sem_glosa AS (
    SELECT
        *,
    FROM
        
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`dashboard_subsidio_sppo`.`sumario_servico_dia_tipo_sem_glosa` where data between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00') and data < date('2024-08-16'))
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
    sumario_servico_dia_tipo_sem_glosa AS s
LEFT JOIN
    subsidio_valor_km_tipo_viagem AS p
ON
    s.data BETWEEN p.data_inicio
    AND p.data_fim
WHERE
    NOT(ROUND(valor_total_subsidio/subsidio_km_teto,2) <= ROUND(distancia_total_subsidio+0.01,2))