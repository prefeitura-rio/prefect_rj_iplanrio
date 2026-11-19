-- Atualiza protocolo_documento_sei formatado em lote para consulta SOAP pelo MCP.
UPDATE $tabela_processos AS p
SET protocolo_documento_sei = COALESCE(NULLIF(v.protocolo_documento_sei, ''), p.protocolo_documento_sei)
FROM (VALUES %s) AS v(processo_id, protocolo_documento_sei)
WHERE p.processo_id = v.processo_id::uuid
  AND (p.protocolo_documento_sei IS NULL OR btrim(p.protocolo_documento_sei) = '');
