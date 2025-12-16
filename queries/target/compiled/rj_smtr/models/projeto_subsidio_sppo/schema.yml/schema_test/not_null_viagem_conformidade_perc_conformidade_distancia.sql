
SELECT
    perc_conformidade_distancia
FROM
    `rj-smtr`.`projeto_subsidio_sppo`.`viagem_conformidade`
WHERE
     = (SELECT MAX() FROM `rj-smtr`.`projeto_subsidio_sppo`.`viagem_conformidade`)
AND
    perc_conformidade_distancia is null
