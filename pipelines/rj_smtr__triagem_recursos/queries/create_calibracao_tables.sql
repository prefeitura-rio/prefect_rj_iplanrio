-- =============================================================================
-- TABELA: goldenset_processos
-- Guarda os processos que possuem validação humana estrita (730 iniciais + revisões de Jaristas)
-- =============================================================================
CREATE TABLE IF NOT EXISTS goldenset_processos (
    goldenset_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    numero_processo_sei VARCHAR(64) UNIQUE NOT NULL,
    numero_auto_infracao VARCHAR(32),
    ouro_hipotese VARCHAR(64) NOT NULL,
    ouro_encaminhamento VARCHAR(64),
    origem_validacao VARCHAR(32) NOT NULL DEFAULT 'GOLDENSET_INICIAL',
    validado_por VARCHAR(128) NOT NULL DEFAULT 'SMTR_AUDITORIA_HISTORICA',
    validado_em TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- As 8 features ativas apuradas no processo
    feat_fuzz_ratio DOUBLE PRECISION,
    feat_token_containment DOUBLE PRECISION,
    feat_exact_match DOUBLE PRECISION,
    feat_adversarial_density DOUBLE PRECISION,
    feat_sim_centroid_pred DOUBLE PRECISION,
    feat_mahalanobis_dist DOUBLE PRECISION,
    feat_is_manuscrito DOUBLE PRECISION,
    feat_tesseract_mean_conf DOUBLE PRECISION,
    
    -- Métricas de calibração associadas
    acertou_hipotese_n1 BOOLEAN NOT NULL DEFAULT TRUE,
    score_nao_conformidade DOUBLE PRECISION,
    
    CONSTRAINT chk_origem_validacao CHECK (origem_validacao IN ('GOLDENSET_INICIAL', 'AUDITORIA_JARISTA'))
);

CREATE INDEX IF NOT EXISTS idx_goldenset_ouro_hipotese ON goldenset_processos (ouro_hipotese);
CREATE INDEX IF NOT EXISTS idx_goldenset_origem ON goldenset_processos (origem_validacao);

-- =============================================================================
-- TABELA: configuracoes_calibracao
-- Armazena os parâmetros matemáticos puros e a representação textual do LightGBM
-- =============================================================================
CREATE TABLE IF NOT EXISTS configuracoes_calibracao (
    calibracao_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    versao_modelo VARCHAR(64) NOT NULL,
    ativo BOOLEAN NOT NULL DEFAULT FALSE,
    criado_em TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    total_amostras_treino INT NOT NULL,
    brier_score_treino DOUBLE PRECISION,
    log_loss_treino DOUBLE PRECISION,
    
    -- Parâmetros estatísticos de pré-processamento (JSONB)
    medianas_imputacao JSONB NOT NULL,
    medias_scaler JSONB NOT NULL,
    desvios_scaler JSONB NOT NULL,
    
    -- Parâmetros da Predição Conforme (CCCP) e Risco Conformal (CRC)
    quantis_conformais_cccp JSONB NOT NULL,
    quantil_pool_fallback DOUBLE PRECISION NOT NULL,
    limiar_crc_grounding DOUBLE PRECISION NOT NULL,
    alpha_conformal DOUBLE PRECISION NOT NULL DEFAULT 0.05,
    
    -- Centróides semânticos históricos em formato serializado
    estatisticas_classes JSONB,
    
    -- Modelo LightGBM serializado no formato texto puro do Booster
    lightgbm_model_text TEXT NOT NULL
);

-- Garante que exista apenas 1 configuração ativa por vez
CREATE UNIQUE INDEX IF NOT EXISTS idx_config_calibracao_unica_ativa 
ON configuracoes_calibracao (ativo) 
WHERE ativo = TRUE;