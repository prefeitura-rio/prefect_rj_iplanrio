
SELECT
    inicio_periodo
FROM
    `rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada`
WHERE
     = (SELECT MAX() FROM `rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada`)
AND
    inicio_periodo is null
