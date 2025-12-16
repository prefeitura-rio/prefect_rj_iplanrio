

SELECT
  *
FROM
  `rj-smtr`.`veiculo`.`infracao`
WHERE
  modo = 'ONIBUS'
  AND placa IS NOT NULL