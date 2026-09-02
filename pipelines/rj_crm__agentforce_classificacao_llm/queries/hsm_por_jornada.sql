-- Catálogo de HSMs por jornada/atividade — usado só pra pegar hsm_botoes_json (detecta
-- resposta atrasada a botão, ver _resposta_e_botao_atrasado em tasks/extract.py). O texto
-- do HSM (hsm_texto) e as variáveis de personalização (hsm_variaveis_json) NÃO vêm mais
-- daqui: são resolvidos direto em v2_chatbot_conversas, por disparo específico (mais
-- preciso que um lookup por jornada) — ver hsm_candidatas em extract_sessoes.sql.
-- hsm_botoes_json é a única coisa que fica só nesse catálogo: é resolvido no intermediate
-- (int_chatbot_v2_mensagens_enviadas_enriquecidas, repo queries-rj-crm-registry) mas
-- descartado antes de chegar em fct_chatbot_v2/v2_chatbot_conversas.
-- Adaptada de clustering/queries/query_hsm_por_jornada.sql.
SELECT
    jornada_nome,
    atividade_nome,
    hsm_nome,
    hsm_botoes_json,
    template_pos_versao_indicador
FROM `{hsm_catalog_table}`
WHERE
    jornada_nome IN ({jornadas})
    AND atividade_versao_mais_recente_indicador = true
