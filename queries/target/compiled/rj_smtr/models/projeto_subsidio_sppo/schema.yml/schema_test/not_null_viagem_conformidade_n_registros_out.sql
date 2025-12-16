
SELECT
    n_registros_out
FROM
    `rj-smtr`.`projeto_subsidio_sppo`.`viagem_conformidade`
WHERE
     = (SELECT MAX() FROM `rj-smtr`.`projeto_subsidio_sppo`.`viagem_conformidade`)
AND
    n_registros_out is null
