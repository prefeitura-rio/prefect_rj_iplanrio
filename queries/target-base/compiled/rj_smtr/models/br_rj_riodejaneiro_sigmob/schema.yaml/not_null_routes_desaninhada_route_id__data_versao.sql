
SELECT
    route_id
FROM
    `rj-smtr`.`br_rj_riodejaneiro_sigmob`.`routes_desaninhada`
WHERE
    data_versao = (SELECT MAX(data_versao) FROM `rj-smtr`.`br_rj_riodejaneiro_sigmob`.`routes_desaninhada`)
AND
    route_id is null
