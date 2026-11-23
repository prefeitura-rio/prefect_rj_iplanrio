-- Sessões relevantes da tabela destino pro recálculo de tema/motivo
-- (tasks/taxonomia.py::extrai_sessoes_para_recalculo_taxonomia): o texto das regras
-- (resumo pra tema, motivo pra motivo), o agrupamento da sessão e os julgamentos atuais,
-- que o recálculo reaproveita quando regra, modelo e prompt do juiz não mudaram.
SELECT
    id_sessao, resumo, motivo, macrotema_principal, conteudo_relevante,
    tema_julgamento, motivo_julgamento, modelo_juiz, prompt_versao_juiz
FROM `{full_id}`
WHERE conteudo_relevante = true
