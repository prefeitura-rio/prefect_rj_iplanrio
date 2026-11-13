-- Insere ou atualiza metadados de processo administrativo ingerido via SEI-Rio.
INSERT INTO $tabela_processos (
    numero_processo_sei,
    id_procedimento_sei,
    id_documento_sei,
    numero_auto_infracao,
    origem_lote,
    data_autuacao
) VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (numero_processo_sei) DO UPDATE SET
    id_procedimento_sei = COALESCE(EXCLUDED.id_procedimento_sei, $tabela_processos.id_procedimento_sei),
    id_documento_sei = COALESCE(EXCLUDED.id_documento_sei, $tabela_processos.id_documento_sei),
    numero_auto_infracao = COALESCE(NULLIF(EXCLUDED.numero_auto_infracao, 'AUTO_SEI'), $tabela_processos.numero_auto_infracao),
    data_autuacao = COALESCE(EXCLUDED.data_autuacao, $tabela_processos.data_autuacao)
RETURNING processo_id;
