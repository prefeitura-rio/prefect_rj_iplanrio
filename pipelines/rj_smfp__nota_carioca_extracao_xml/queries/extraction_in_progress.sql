SELECT
    tarefa,
    tipo,
    status,
    data_inicio,
    data_termino,
    execucao_progresso,
    execucao_resumo,
    ROUND((SYSDATE - data_inicio) * 86400) AS segundos_em_execucao
FROM nfse_owner.tarefas
WHERE tipo = :tipo_extracao
  AND status IN (0, 1, 4)
ORDER BY tarefa DESC
FETCH FIRST 1 ROWS ONLY
