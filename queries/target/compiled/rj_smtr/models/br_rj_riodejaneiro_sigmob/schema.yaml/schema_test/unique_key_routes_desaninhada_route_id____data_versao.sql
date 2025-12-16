
SELECT 
    *
FROM (
    SELECT
        route_id,
        data_versao,
        ROW_NUMBER() over (partition by route_id) rn
    FROM
        `rj-smtr`.`br_rj_riodejaneiro_sigmob`.`routes_desaninhada`
    WHERE
        data_versao = (select max(data_versao) from `rj-smtr`.`br_rj_riodejaneiro_sigmob`.`routes_desaninhada`)
)

WHERE rn>1
