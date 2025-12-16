





  
    

    
  


WITH staging_data AS (
  SELECT
    data_viagem AS data,
    datetime_partida,
    datetime_chegada,
    datetime_processamento,
    timestamp_captura AS datetime_captura,
    id_veiculo,
    trip_id,
    route_id,
    shape_id,
    servico,
    CASE
      WHEN sentido = 'I' THEN 'Ida'
      WHEN sentido = 'V' THEN 'Volta'
      ELSE sentido
    END AS sentido,
    id_viagem
  FROM
    `rj-smtr`.`br_rj_riodejaneiro_viagem_zirix_staging`.`viagem_informada`
  
    WHERE
      
  DATE(data) BETWEEN DATE("2022-01-01T00:00:00") AND DATE("2022-01-01T01:00:00")

  
),
complete_partitions AS (
  SELECT
    *,
    0 AS priority
  FROM
    staging_data

  
),
deduplicado AS (
  SELECT
    * EXCEPT(rn, priority)
  FROM
    (
      SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY id_viagem ORDER BY datetime_captura DESC, priority) AS rn
      FROM
        complete_partitions
    )
  WHERE
    rn = 1
)
SELECT
  *,
  '' AS versao,
  CURRENT_DATETIME("America/Sao_Paulo") as datetime_ultima_atualizacao
FROM
  deduplicado