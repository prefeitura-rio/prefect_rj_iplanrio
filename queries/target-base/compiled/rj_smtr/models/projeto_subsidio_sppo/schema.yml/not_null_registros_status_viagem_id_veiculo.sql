
SELECT
    id_veiculo
FROM
    `rj-smtr`.`projeto_subsidio_sppo`.`registros_status_viagem`
WHERE
     = (SELECT MAX() FROM `rj-smtr`.`projeto_subsidio_sppo`.`registros_status_viagem`)
AND
    id_veiculo is null
