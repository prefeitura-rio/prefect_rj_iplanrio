
SELECT
    distancia_planejada
FROM
    `rj-smtr`.`projeto_subsidio_sppo`.`registros_status_viagem`
WHERE
     = (SELECT MAX() FROM `rj-smtr`.`projeto_subsidio_sppo`.`registros_status_viagem`)
AND
    distancia_planejada is null
