
SELECT
    versao_modelo
FROM
    `rj-smtr`.`projeto_subsidio_sppo`.`viagem_completa`
WHERE
     = (SELECT MAX() FROM `rj-smtr`.`projeto_subsidio_sppo`.`viagem_completa`)
AND
    versao_modelo is null
