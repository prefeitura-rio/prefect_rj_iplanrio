
SELECT
    *
FROM (
    SELECT
        trip_id,
        data_versao,
        ROW_NUMBER() over (partition by trip_id) rn
    FROM
        (select * from `rj-smtr`.`br_rj_riodejaneiro_sigmob`.`trips_desaninhada` where DATA BETWEEN DATE('2022-01-01T00:00:00') AND DATE('2022-01-01T01:00:00')) dbt_subquery
    WHERE
        data_versao = (select max(data_versao) from (select * from `rj-smtr`.`br_rj_riodejaneiro_sigmob`.`trips_desaninhada` where DATA BETWEEN DATE('2022-01-01T00:00:00') AND DATE('2022-01-01T01:00:00')) dbt_subquery)
)

WHERE rn>1
