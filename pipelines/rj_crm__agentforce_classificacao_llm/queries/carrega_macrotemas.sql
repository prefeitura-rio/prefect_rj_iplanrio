-- Macrotemas da taxonomia de roteamento, na ordem do prompt
-- (tasks/roteamento.py::carrega_taxonomia_roteamento).
SELECT nome, descricao, secretarias, prompt_versao_macrotema
FROM `${full_id}`
ORDER BY ordem
