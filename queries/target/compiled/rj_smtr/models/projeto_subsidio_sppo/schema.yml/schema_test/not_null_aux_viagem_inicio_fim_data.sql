
SELECT
    data
FROM
    `rj-smtr`.`projeto_subsidio_sppo`.`aux_viagem_inicio_fim`
WHERE
     = (SELECT MAX() FROM `rj-smtr`.`projeto_subsidio_sppo`.`aux_viagem_inicio_fim`)
AND
    data is null
