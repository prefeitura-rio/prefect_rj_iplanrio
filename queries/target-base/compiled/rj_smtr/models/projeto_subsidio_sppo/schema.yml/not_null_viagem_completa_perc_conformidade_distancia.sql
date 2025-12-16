
SELECT
    perc_conformidade_distancia
FROM
    `rj-smtr`.`projeto_subsidio_sppo`.`viagem_completa`
WHERE
     = (SELECT MAX() FROM `rj-smtr`.`projeto_subsidio_sppo`.`viagem_completa`)
AND
    perc_conformidade_distancia is null
