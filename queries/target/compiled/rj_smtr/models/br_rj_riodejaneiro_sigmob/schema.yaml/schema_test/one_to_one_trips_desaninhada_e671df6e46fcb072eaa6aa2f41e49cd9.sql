




with t as (
SELECT
    m.trip_id from_col,
    n.trip_id to_col
FROM (
    SELECT
        trip_id
    FROM `rj-smtr`.`br_rj_riodejaneiro_sigmob`.`trips_desaninhada`
    WHERE data_versao = "2022-09-12"
) m
LEFT JOIN (
    SELECT
        trip_id
    FROM `rj-smtr`.`br_rj_riodejaneiro_sigmob`.`shapes_geom`
    WHERE data_versao = "2022-08-14"
) n
ON m.trip_id = n.trip_id
)
SELECT *
FROM (
    SELECT 
        from_col,
        count(to_col) ct
    FROM t
    GROUP BY from_col
) 
where ct != 1
