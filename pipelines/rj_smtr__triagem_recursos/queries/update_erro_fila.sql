-- Atualiza fila_trabalho em lote para falhas de processamento com mensagem de erro.
UPDATE $tabela_fila AS f
SET
    status = v.status,
    tentativas = COALESCE(f.tentativas, 0) + 1,
    ultima_mensagem_erro = NULLIF(v.ultima_mensagem_erro, ''),
    atualizado_em = CURRENT_TIMESTAMP
FROM (VALUES %s) AS v(processo_id, status, ultima_mensagem_erro)
WHERE f.processo_id = v.processo_id::uuid;
