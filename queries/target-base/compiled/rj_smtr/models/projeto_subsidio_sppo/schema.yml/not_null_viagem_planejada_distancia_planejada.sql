
SELECT
    distancia_planejada
FROM
    `rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada`
WHERE
     = (SELECT MAX() FROM `rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada`)
AND
    distancia_planejada is null
