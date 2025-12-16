

SELECT
  PARSE_DATE("%d/%m/%Y", DATA) AS data,
  id_recurso,
  tipo_recurso,
  servico,
FROM
  `rj-smtr-staging`.`br_rj_riodejaneiro_recursos_staging`.`recursos_sppo_servico_dia_avaliacao`