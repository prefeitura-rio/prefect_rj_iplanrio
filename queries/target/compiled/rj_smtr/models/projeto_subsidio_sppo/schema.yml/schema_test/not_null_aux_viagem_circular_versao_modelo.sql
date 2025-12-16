
SELECT
    versao_modelo
FROM
    `rj-smtr`.`projeto_subsidio_sppo`.`aux_viagem_circular`
WHERE
     = (SELECT MAX() FROM `rj-smtr`.`projeto_subsidio_sppo`.`aux_viagem_circular`)
AND
    versao_modelo is null
