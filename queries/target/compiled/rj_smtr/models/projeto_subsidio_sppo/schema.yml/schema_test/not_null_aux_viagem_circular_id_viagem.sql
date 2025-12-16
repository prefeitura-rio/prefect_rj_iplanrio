
SELECT
    id_viagem
FROM
    `rj-smtr`.`projeto_subsidio_sppo`.`aux_viagem_circular`
WHERE
     = (SELECT MAX() FROM `rj-smtr`.`projeto_subsidio_sppo`.`aux_viagem_circular`)
AND
    id_viagem is null
