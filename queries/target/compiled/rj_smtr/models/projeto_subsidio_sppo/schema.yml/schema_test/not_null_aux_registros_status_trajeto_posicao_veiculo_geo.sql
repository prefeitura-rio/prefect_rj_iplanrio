
SELECT
    posicao_veiculo_geo
FROM
    `rj-smtr`.`projeto_subsidio_sppo`.`aux_registros_status_trajeto`
WHERE
     = (SELECT MAX() FROM `rj-smtr`.`projeto_subsidio_sppo`.`aux_registros_status_trajeto`)
AND
    posicao_veiculo_geo is null
