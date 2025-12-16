


SELECT
  d.id_operadora,
  cpj.nm_contato AS contato,
  cpj.nr_ramal AS ramal,
  COALESCE(cpj.nr_telefone, c.nr_telefone) AS telefone,
  COALESCE(cpj.tx_email, c.tx_email) AS email
FROM
  `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`cliente` AS c
LEFT JOIN
  `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`contato_pessoa_juridica` cpj
ON
  c.cd_cliente = cpj.cd_cliente
JOIN
  `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`operadora_transporte` AS ot
ON
  ot.cd_cliente = c.cd_cliente
JOIN
  `rj-smtr`.`cadastro`.`operadoras` d
ON d.id_operadora_jae = ot.cd_operadora_transporte