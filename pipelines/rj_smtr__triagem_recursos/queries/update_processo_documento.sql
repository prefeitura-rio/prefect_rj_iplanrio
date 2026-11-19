-- Preenche id_documento_sei em lote em processos_administrativos caso ainda esteja vazio.
UPDATE $tabela_processos AS p
SET id_documento_sei = COALESCE(NULLIF(v.id_documento_sei, ''), p.id_documento_sei)
FROM (VALUES %s) AS v(processo_id, id_documento_sei)
WHERE p.processo_id = v.processo_id::uuid
  AND (p.id_documento_sei IS NULL OR btrim(p.id_documento_sei) = '');
