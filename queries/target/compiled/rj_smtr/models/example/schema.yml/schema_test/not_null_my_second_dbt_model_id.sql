
SELECT
    id
FROM
    `rj-smtr`.`dbt`.`my_second_dbt_model`
WHERE
     = (SELECT MAX() FROM `rj-smtr`.`dbt`.`my_second_dbt_model`)
AND
    id is null
