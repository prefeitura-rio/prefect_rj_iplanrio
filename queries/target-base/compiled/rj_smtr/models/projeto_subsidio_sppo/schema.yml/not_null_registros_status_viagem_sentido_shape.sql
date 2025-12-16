
SELECT
    sentido_shape
FROM
    `rj-smtr`.`projeto_subsidio_sppo`.`registros_status_viagem`
WHERE
     = (SELECT MAX() FROM `rj-smtr`.`projeto_subsidio_sppo`.`registros_status_viagem`)
AND
    sentido_shape is null
