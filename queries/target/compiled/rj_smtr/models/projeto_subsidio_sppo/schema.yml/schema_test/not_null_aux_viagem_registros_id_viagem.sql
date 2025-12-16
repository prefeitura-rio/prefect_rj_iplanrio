
SELECT
    id_viagem
FROM
    `rj-smtr`.`projeto_subsidio_sppo`.`aux_viagem_registros`
WHERE
     = (SELECT MAX() FROM `rj-smtr`.`projeto_subsidio_sppo`.`aux_viagem_registros`)
AND
    id_viagem is null
