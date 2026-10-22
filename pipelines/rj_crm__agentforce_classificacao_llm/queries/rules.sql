SELECT secretaria, categoria_pai, nome_funcao, nome, regra_python
FROM `{{ table }}`
WHERE etapa = @etapa AND ativo_indicador = TRUE
