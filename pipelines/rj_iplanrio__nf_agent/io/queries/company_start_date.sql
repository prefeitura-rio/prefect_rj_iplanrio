-- Company opening date (inicio_atividade_data) for a given CNPJ.
-- cnpj_param is bound as a BigQuery query parameter (INT64), not templated.
SELECT inicio_atividade_data
FROM `$project.$dataset.bcadastro_cnpj_recorte`
WHERE cnpj = @cnpj_param
LIMIT 1
