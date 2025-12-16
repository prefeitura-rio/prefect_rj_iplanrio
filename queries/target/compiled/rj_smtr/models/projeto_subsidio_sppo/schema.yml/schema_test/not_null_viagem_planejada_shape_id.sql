
SELECT
    shape_id
FROM
    `rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada`
WHERE
     = (SELECT MAX() FROM `rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada`)
AND
    shape_id is null
