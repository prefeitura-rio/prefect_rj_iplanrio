with t as (
SELECT
    m.route_id from_col,
    n.route_id to_col
FROM (
    SELECT
        route_id
    FROM `rj-smtr`.`br_rj_riodejaneiro_sigmob`.`trips_desaninhada`
    WHERE data_versao = "2022-09-12"
) m
LEFT JOIN (
    SELECT
        route_id
    FROM `rj-smtr`.`br_rj_riodejaneiro_sigmob`.`routes_desaninhada`
    WHERE data_versao = "2022-09-12"
) n
ON m.route_id = n.route_id
)

SELECT 
    *
FROM t
WHERE to_col is null
