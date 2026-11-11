-- Preenche id_documento_sei em processos_administrativos caso ainda esteja vazio.
UPDATE $tabela_processos
SET id_documento_sei = %s
WHERE processo_id = %s::uuid
  AND (id_documento_sei IS NULL OR btrim(id_documento_sei) = '');
