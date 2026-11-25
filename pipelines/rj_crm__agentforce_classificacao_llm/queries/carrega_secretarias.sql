-- Secretarias da taxonomia de roteamento, na ordem do prompt
-- (tasks/roteamento.py::carrega_taxonomia_roteamento).
SELECT sigla, nome, descricao, prompt_versao_secretaria
FROM `${full_id}`
ORDER BY ordem
