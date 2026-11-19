UPDATE $tabela_fila
SET
    status = %s,
    ultima_mensagem_erro = NULL,
    atualizado_em = CURRENT_TIMESTAMP
WHERE processo_id = ANY(%s::uuid[]);
