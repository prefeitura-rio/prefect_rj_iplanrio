SELECT
    tarefa,
    tipo,
    data_inicio,
    ROUND((SYSDATE - data_inicio) * 86400) AS segundos_em_execucao
FROM nfse_owner.tarefas
WHERE tipo IN (:1, :2)
  AND status = :3
ORDER BY data_inicio
