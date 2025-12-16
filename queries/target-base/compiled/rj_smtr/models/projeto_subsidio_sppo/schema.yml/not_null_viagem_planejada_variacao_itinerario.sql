
SELECT
    variacao_itinerario
FROM
    `rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada`
WHERE
     = (SELECT MAX() FROM `rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada`)
AND
    variacao_itinerario is null
