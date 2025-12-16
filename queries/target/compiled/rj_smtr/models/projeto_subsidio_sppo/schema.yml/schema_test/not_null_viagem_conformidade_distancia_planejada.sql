
SELECT
    distancia_planejada
FROM
    `rj-smtr`.`projeto_subsidio_sppo`.`viagem_conformidade`
WHERE
     = (SELECT MAX() FROM `rj-smtr`.`projeto_subsidio_sppo`.`viagem_conformidade`)
AND
    distancia_planejada is null
