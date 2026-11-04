SELECT NULL AS secretaria, agrupamento_tipo, agrupamento, categoria_pai, nome_funcao, nome, regra_python
FROM `{{ table }}`
WHERE etapa = @etapa AND agrupamento_tipo = 'macrotema' AND ativo_indicador = TRUE
