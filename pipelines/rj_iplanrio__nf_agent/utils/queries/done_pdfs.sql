SELECT DISTINCT nome_arquivo
FROM `$table`
WHERE versao_pipeline.versao_processamento = @versao_processamento
  AND nome_arquivo IN UNNEST(@nomes)
