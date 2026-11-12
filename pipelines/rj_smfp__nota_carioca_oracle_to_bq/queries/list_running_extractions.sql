SELECT
    tarefa,
    tipo,
    data_inicio,
    ROUND((SYSDATE - data_inicio) * 86400) AS segundos_em_execucao
FROM nfse_owner.tarefas
WHERE tipo IN (:tipo_extracao, :tipo_pos_processamento)
  AND status = :status_em_execucao
ORDER BY data_inicio
