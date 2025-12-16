

SELECT
  *
FROM
  `rj-smtr`.`veiculo`.`licenciamento`
WHERE
  tipo_veiculo NOT LIKE "%ROD%"
  and modo = 'ONIBUS'