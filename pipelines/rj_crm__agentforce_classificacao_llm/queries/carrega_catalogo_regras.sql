-- Regras ativas de UMA etapa ('tema' ou 'motivo') de um agrupamento_tipo (hoje 'macrotema')
-- — tasks/taxonomia.py::carrega_catalogo_regras. descricao/inclui/nao_inclui vão no prompt
-- do juiz (tasks/juiz.py); criado_em é a versão da regra (o recálculo só reaproveita
-- julgamento da mesma versão).
SELECT
    agrupamento_tipo, agrupamento, categoria_pai, nome_funcao, nome, descricao, inclui, nao_inclui,
    regra_python, criado_em
FROM `${full_id}`
WHERE etapa = @etapa AND agrupamento_tipo = @agrupamento_tipo AND ativo_indicador = TRUE
