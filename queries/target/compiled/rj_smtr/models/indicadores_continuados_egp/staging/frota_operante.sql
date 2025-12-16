

SELECT
  MIN(data) AS data,
  EXTRACT(YEAR FROM data) AS ano,
  EXTRACT(MONTH FROM data) AS mes,
  "Ônibus" AS modo,
  COUNT(DISTINCT id_veiculo) AS quantidade_veiculo_mes,
  CURRENT_DATE("America/Sao_Paulo") AS data_ultima_atualizacao,
  '' as versao
FROM
  `rj-smtr`.`projeto_subsidio_sppo`.`viagem_completa`
  --rj-smtr.projeto_subsidio_sppo.viagem_completa
WHERE

  data BETWEEN DATE_TRUNC(DATE("2022-01-01T01:00:00"), MONTH)
  AND LAST_DAY(DATE("2022-01-01T01:00:00"), MONTH)
  AND data < DATE_TRUNC(CURRENT_DATE("America/Sao_Paulo"), MONTH)

GROUP BY
  2,
  3