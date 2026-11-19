-- MERGE parcial do recálculo (tasks/taxonomia.py::atualiza_tema_motivo): só as colunas de
-- tema/motivo e do juiz, por id_sessao — nenhuma outra coluna da destino é tocada.
MERGE `{destino_full}` AS t
USING `{tmp_full}` AS s
ON t.id_sessao = s.id_sessao
WHEN MATCHED THEN
    UPDATE SET
        t.tema_nome = s.tema_nome,
        t.tema_motivo = s.tema_motivo,
        t.tema_julgamento = s.tema_julgamento,
        t.tema_motivo_julgamento = s.tema_motivo_julgamento,
        t.modelo_juiz = s.modelo_juiz,
        t.prompt_versao_juiz = s.prompt_versao_juiz
