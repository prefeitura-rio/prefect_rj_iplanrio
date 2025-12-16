
SELECT
    tipo_dia
FROM
    `rj-smtr`.`projeto_subsidio_sppo`.`viagem_completa`
WHERE
     = (SELECT MAX() FROM `rj-smtr`.`projeto_subsidio_sppo`.`viagem_completa`)
AND
    tipo_dia is null
