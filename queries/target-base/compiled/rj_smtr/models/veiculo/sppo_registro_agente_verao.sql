


  



SELECT
  * EXCEPT(rn)
FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY id_registro ORDER BY datetime_captura DESC) AS rn
    FROM
      `rj-smtr`.`veiculo_staging`.`sppo_registro_agente_verao`
    WHERE
      data = DATE('2024-09-22')
      AND validacao = TRUE
  )
WHERE rn = 1