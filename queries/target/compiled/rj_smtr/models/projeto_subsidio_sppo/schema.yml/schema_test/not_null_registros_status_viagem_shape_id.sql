
SELECT
    shape_id
FROM
    `rj-smtr`.`projeto_subsidio_sppo`.`registros_status_viagem`
WHERE
     = (SELECT MAX() FROM `rj-smtr`.`projeto_subsidio_sppo`.`registros_status_viagem`)
AND
    shape_id is null
