
SELECT
    consorcio
FROM
    `rj-smtr`.`projeto_subsidio_sppo`.`viagem_completa`
WHERE
     = (SELECT MAX() FROM `rj-smtr`.`projeto_subsidio_sppo`.`viagem_completa`)
AND
    consorcio is null
