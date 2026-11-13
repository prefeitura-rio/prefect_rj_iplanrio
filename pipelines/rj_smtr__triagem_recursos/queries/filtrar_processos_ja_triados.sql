SELECT p.numero_processo_sei
FROM $tabela_processos p
INNER JOIN $tabela_fila f ON p.processo_id = f.processo_id
WHERE p.numero_processo_sei = ANY(%s)
  AND f.status = %s;
