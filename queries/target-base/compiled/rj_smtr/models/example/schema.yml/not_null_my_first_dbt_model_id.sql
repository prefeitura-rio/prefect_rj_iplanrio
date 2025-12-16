
SELECT
    id
FROM
    `rj-smtr`.`dbt`.`my_first_dbt_model`
WHERE
     = (SELECT MAX() FROM `rj-smtr`.`dbt`.`my_first_dbt_model`)
AND
    id is null
