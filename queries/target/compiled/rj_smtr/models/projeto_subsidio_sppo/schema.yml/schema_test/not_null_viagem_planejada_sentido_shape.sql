
SELECT
    sentido_shape
FROM
    `rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada`
WHERE
     = (SELECT MAX() FROM `rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada`)
AND
    sentido_shape is null
