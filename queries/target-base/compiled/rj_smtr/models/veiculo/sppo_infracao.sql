
  
WITH
  infracao AS (
    SELECT
      * EXCEPT(data),
      SAFE_CAST(data AS DATE) AS data
    FROM
      `rj-smtr`.`veiculo_staging`.`sppo_infracao` as t
    WHERE
      DATE(data) = DATE("2023-03-10")
  ),
  infracao_rn AS (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY data, id_auto_infracao) rn
    FROM
      infracao
  )
SELECT
  * EXCEPT(rn)
FROM
  infracao_rn
WHERE
  rn = 1