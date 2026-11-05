UPDATE $tabela_fila
SET 
    status = %s,
    atualizado_em = CURRENT_TIMESTAMP
WHERE processo_id = ANY(%s::uuid[]);
