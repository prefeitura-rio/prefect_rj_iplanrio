-- Enfileira um processo na fila de trabalho caso ainda não esteja presente.
INSERT INTO $tabela_fila (processo_id, status)
SELECT %s, %s
WHERE NOT EXISTS (
    SELECT 1 FROM $tabela_fila WHERE processo_id = %s
);
