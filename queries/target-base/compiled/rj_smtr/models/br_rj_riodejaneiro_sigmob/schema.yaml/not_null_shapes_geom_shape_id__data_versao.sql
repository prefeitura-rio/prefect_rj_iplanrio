
SELECT
    shape_id
FROM
    `rj-smtr`.`br_rj_riodejaneiro_sigmob`.`shapes_geom`
WHERE
    data_versao = (SELECT MAX(data_versao) FROM `rj-smtr`.`br_rj_riodejaneiro_sigmob`.`shapes_geom`)
AND
    shape_id is null
