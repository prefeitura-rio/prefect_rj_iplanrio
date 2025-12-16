
SELECT
    distancia_aferida
FROM
    `rj-smtr`.`projeto_subsidio_sppo`.`aux_viagem_registros`
WHERE
     = (SELECT MAX() FROM `rj-smtr`.`projeto_subsidio_sppo`.`aux_viagem_registros`)
AND
    distancia_aferida is null
