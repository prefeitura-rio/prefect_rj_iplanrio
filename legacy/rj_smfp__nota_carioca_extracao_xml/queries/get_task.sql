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
WHERE tarefa = :tarefa
