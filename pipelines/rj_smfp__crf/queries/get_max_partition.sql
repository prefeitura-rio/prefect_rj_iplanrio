-- Query para obter a maior data de partição da tabela CRF no BigQuery.
-- O resultado é comparado com as datas extraídas dos nomes dos arquivos ZIP no GCS.
-- Apenas arquivos com data maior que a partição máxima serão processados.

SELECT
    MAX(data_particao) AS max_data_particao
FROM
    `${project_id}.${dataset_id}.${table_id}`
