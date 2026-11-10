-- Enfileira um processo na fila de trabalho caso ainda não esteja presente.
INSERT INTO $tabela_fila (processo_id, status)
VALUES (%s, %s)
ON CONFLICT (processo_id) DO NOTHING;
