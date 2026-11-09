-- Registra evento de auditoria operacional do processo administrativo.
INSERT INTO $tabela_eventos (processo_id, usuario_sei, tipo_evento, detalhes_json)
VALUES (%s, %s, %s, %s);
