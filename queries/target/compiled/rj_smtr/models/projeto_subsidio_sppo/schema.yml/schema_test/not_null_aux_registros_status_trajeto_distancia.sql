
SELECT
    distancia
FROM
    `rj-smtr`.`projeto_subsidio_sppo`.`aux_registros_status_trajeto`
WHERE
     = (SELECT MAX() FROM `rj-smtr`.`projeto_subsidio_sppo`.`aux_registros_status_trajeto`)
AND
    distancia is null
