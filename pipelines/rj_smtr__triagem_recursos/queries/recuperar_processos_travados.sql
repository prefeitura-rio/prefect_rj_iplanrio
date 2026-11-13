-- Recupera itens que ficaram presos em PROCESSANDO_IA alem do tempo limite
UPDATE $tabela_fila
SET status = 'PENDENTE_IA',
    tentativas = tentativas + 1,
    ultima_mensagem_erro = 'Timeout de processamento: resetado automaticamente para PENDENTE_IA',
    atualizado_em = CURRENT_TIMESTAMP
WHERE status = 'PROCESSANDO_IA'
  AND atualizado_em < CURRENT_TIMESTAMP - (%s * INTERVAL '1 minute');
