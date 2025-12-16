
SELECT
    n_registros_shape
FROM
    `rj-smtr`.`projeto_subsidio_sppo`.`aux_viagem_registros`
WHERE
     = (SELECT MAX() FROM `rj-smtr`.`projeto_subsidio_sppo`.`aux_viagem_registros`)
AND
    n_registros_shape is null
