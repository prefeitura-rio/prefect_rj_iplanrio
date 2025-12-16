
SELECT
    datetime_partida
FROM
    `rj-smtr`.`projeto_subsidio_sppo`.`viagem_conformidade`
WHERE
     = (SELECT MAX() FROM `rj-smtr`.`projeto_subsidio_sppo`.`viagem_conformidade`)
AND
    datetime_partida is null
