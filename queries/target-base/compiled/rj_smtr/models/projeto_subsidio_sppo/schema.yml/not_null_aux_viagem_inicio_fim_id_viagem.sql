
SELECT
    id_viagem
FROM
    `rj-smtr`.`projeto_subsidio_sppo`.`aux_viagem_inicio_fim`
WHERE
     = (SELECT MAX() FROM `rj-smtr`.`projeto_subsidio_sppo`.`aux_viagem_inicio_fim`)
AND
    id_viagem is null
