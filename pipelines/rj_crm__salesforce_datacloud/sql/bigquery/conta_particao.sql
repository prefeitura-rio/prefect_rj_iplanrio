-- Validação pós-carga: linhas da partição carregada na tabela final.
SELECT COUNT(*) AS cnt
FROM `{table}`
WHERE {partition_field} = '{partition_date}'
