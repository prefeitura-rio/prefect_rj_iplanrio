-- Query to get the maximum measurement date from precipitacao_alertario_5min
-- This query returns the latest date_medicao from the staging table
-- The result will be compared with dates in the bucket with 'Chuvas_' suffix
-- Only dates greater than those in the bucket will be returned

SELECT
    MAX(data_medicao) as max_data_medicao
FROM
    `${project_id}.${dataset_id}.${table_id}`
