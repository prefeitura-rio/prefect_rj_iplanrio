
SELECT 
    *
FROM (
    SELECT
        trip_id,
        data_versao,
        ROW_NUMBER() over (partition by trip_id) rn
    FROM
        `rj-smtr`.`br_rj_riodejaneiro_sigmob`.`trips_desaninhada`
    WHERE
        data_versao = (select max(data_versao) from `rj-smtr`.`br_rj_riodejaneiro_sigmob`.`trips_desaninhada`)
)

WHERE rn>1
