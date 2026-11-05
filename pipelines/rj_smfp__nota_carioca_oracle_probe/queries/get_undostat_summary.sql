SELECT
    COUNT(*)                                                     AS amostras,
    TO_CHAR(MIN(begin_time), 'YYYY-MM-DD HH24:MI')               AS primeira_amostra,
    TO_CHAR(MAX(end_time), 'YYYY-MM-DD HH24:MI')                 AS ultima_amostra,
    MAX(tuned_undoretention)                                     AS max_tuned_undoretention_s,
    MAX(maxquerylen)                                             AS max_maxquerylen_s,
    SUM(ssolderrcnt)                                             AS ora_01555,
    SUM(nospaceerrcnt)                                           AS erros_sem_espaco_undo,
    ROUND(MAX(undoblks / ((end_time - begin_time) * 86400)), 2)  AS pico_blocos_undo_por_s
FROM v$$undostat
WHERE begin_time >= SYSDATE - 7
