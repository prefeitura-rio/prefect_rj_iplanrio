
SELECT
    servico_informado
FROM
    `rj-smtr`.`projeto_subsidio_sppo`.`aux_viagem_circular`
WHERE
     = (SELECT MAX() FROM `rj-smtr`.`projeto_subsidio_sppo`.`aux_viagem_circular`)
AND
    servico_informado is null
