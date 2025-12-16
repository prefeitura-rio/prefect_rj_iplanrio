
SELECT
    datetime_partida
FROM
    `rj-smtr`.`projeto_subsidio_sppo`.`aux_viagem_circular`
WHERE
     = (SELECT MAX() FROM `rj-smtr`.`projeto_subsidio_sppo`.`aux_viagem_circular`)
AND
    datetime_partida is null
