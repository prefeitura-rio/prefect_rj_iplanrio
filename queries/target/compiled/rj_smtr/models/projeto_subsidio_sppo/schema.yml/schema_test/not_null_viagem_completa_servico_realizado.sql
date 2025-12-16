
SELECT
    servico_realizado
FROM
    `rj-smtr`.`projeto_subsidio_sppo`.`viagem_completa`
WHERE
     = (SELECT MAX() FROM `rj-smtr`.`projeto_subsidio_sppo`.`viagem_completa`)
AND
    servico_realizado is null
