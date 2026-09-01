-- Catálogo de HSMs por jornada — usado para enriquecer a sessão extraída com o texto
-- do HSM (`hsm_texto`) e os botões (`hsm_botoes_json`, usado pra detectar resposta
-- atrasada a botão). Copiada de clustering/queries/query_hsm_por_jornada.sql.
SELECT
    jornada_nome,
    atividade_nome,
    hsm_nome,
    hsm_texto,
    hsm_categoria,
    hsm_botoes_json,
    session_texto,
    template_pos_versao_indicador,
    hsm_botoes_json
FROM `{hsm_catalog_table}`
WHERE
    jornada_nome IN ({jornadas})
    AND atividade_versao_mais_recente_indicador = true
