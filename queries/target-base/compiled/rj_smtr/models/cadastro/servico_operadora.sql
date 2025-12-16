

SELECT
  c.id_consorcio,
  c.consorcio,
  o.id_operadora,
  o.operadora,
  lco.cd_linha AS id_servico_jae,
  s.servico,
  s.descricao_servico,
  lco.dt_inicio_validade AS data_inicio_validade,
  lco.dt_fim_validade AS data_fim_validade,
  '' as versao
FROM
  `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`linha_consorcio_operadora_transporte` lco
JOIN
  `rj-smtr`.`cadastro`.`operadoras` o
ON
  lco.cd_operadora_transporte = o.id_operadora_jae
JOIN
  `rj-smtr`.`cadastro`.`consorcios` c
ON
  lco.cd_consorcio = c.id_consorcio_jae
JOIN
  `rj-smtr`.`cadastro`.`servicos` s
ON
  lco.cd_linha = s.id_servico_jae