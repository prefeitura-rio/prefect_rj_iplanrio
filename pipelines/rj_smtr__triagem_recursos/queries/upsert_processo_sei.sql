-- Insere ou atualiza metadados de processo administrativo ingerido via SEI-Rio.
INSERT INTO $tabela_processos (
    numero_processo_sei,
    id_procedimento_sei,
    id_documento_sei,
    numero_auto_infracao,
    origem_lote,
    codigo_jari,
    atribuido_a_login_sei,
    status_distribuicao
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (numero_processo_sei) DO UPDATE SET
    id_procedimento_sei = COALESCE(EXCLUDED.id_procedimento_sei, $tabela_processos.id_procedimento_sei),
    id_documento_sei = COALESCE(EXCLUDED.id_documento_sei, $tabela_processos.id_documento_sei),
    atribuido_a_login_sei = COALESCE(EXCLUDED.atribuido_a_login_sei, $tabela_processos.atribuido_a_login_sei),
    status_distribuicao = EXCLUDED.status_distribuicao
RETURNING processo_id;
