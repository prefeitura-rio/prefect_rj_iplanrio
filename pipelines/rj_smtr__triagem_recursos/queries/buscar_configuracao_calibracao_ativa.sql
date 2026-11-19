SELECT 
    calibracao_id,
    versao_modelo,
    total_amostras_treino,
    brier_score_treino,
    log_loss_treino,
    medianas_imputacao,
    medias_scaler,
    desvios_scaler,
    quantis_conformais_cccp,
    quantil_pool_fallback,
    limiar_crc_grounding,
    alpha_conformal,
    estatisticas_classes,
    lightgbm_model_text
FROM $tabela_calibracao
WHERE ativo = TRUE
LIMIT 1;
