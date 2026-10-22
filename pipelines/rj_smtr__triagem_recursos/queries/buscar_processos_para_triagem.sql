SELECT 
    p.processo_id,
    p.numero_processo_sei,
    p.id_procedimento_sei,
    p.id_documento_sei,
    p.numero_auto_infracao,
    p.nome_arquivo_origem,
    p.data_autuacao
FROM $tabela_processos p
INNER JOIN $tabela_fila f ON p.processo_id = f.processo_id
WHERE f.status = %s
  AND (p.origem_lote = %s OR p.origem_lote = 'SEI_JARI')
  AND (%s::date IS NULL OR p.data_autuacao >= %s::date)
  AND (%s::date IS NULL OR p.data_autuacao <= %s::date)
ORDER BY p.criado_em ASC
LIMIT %s;
