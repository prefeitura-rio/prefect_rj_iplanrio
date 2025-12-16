
SELECT
    trip_id
FROM
    `rj-smtr`.`br_rj_riodejaneiro_sigmob`.`trips_desaninhada`
WHERE
    data_versao = (SELECT MAX(data_versao) FROM `rj-smtr`.`br_rj_riodejaneiro_sigmob`.`trips_desaninhada`)
AND
    trip_id is null
