-- ==============================================================================
-- SISTEMA DE TRIAGEM DE RECURSOS SMTR-RJ / IPLANRIO
-- BANCO DE DADOS: Cloud SQL (PostgreSQL 15+)
-- ==============================================================================

-- Extensões Criptográficas e Utilitárias
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- TABELA
-- Processos Administrativos (Identificação e Ingestão)
CREATE TABLE IF NOT EXISTS processos_administrativos (
    processo_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    numero_processo_sei     VARCHAR(32) NOT NULL UNIQUE,
    id_procedimento_sei     VARCHAR(32),
    id_documento_sei        VARCHAR(64),
    numero_auto_infracao    VARCHAR(20),
    data_autuacao           DATE,
    origem_lote             VARCHAR(20) NOT NULL CHECK (origem_lote IN ('GOLDENSET', 'PASSIVO', 'MENSAL', 'SEI_JARI')),
    nome_arquivo_origem     VARCHAR(255),
    hash_sha256_documento   CHAR(64),
    criado_em               TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_proc_data_autuacao 
ON processos_administrativos (data_autuacao);

CREATE INDEX IF NOT EXISTS idx_proc_origem 
ON processos_administrativos (origem_lote);

CREATE INDEX IF NOT EXISTS idx_proc_doc_sei 
ON processos_administrativos (id_documento_sei);

-- TABELA
-- Predições da Pipeline de IA e Calibração Conformal
CREATE TABLE IF NOT EXISTS predicoes_pipeline (
    predicao_id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    processo_id                 UUID NOT NULL REFERENCES processos_administrativos(processo_id) ON DELETE RESTRICT,
    calibracao_id               UUID,
    modelo_llm                  VARCHAR(64) DEFAULT 'vertex/gemini-3.7-flash' NOT NULL,
    versao_prompt               VARCHAR(64) DEFAULT 'v3_baseline' NOT NULL,
    id_documento_sei            VARCHAR(64),
    n1_classe_predita           VARCHAR(32) NOT NULL,
    n1_encaminhamento           VARCHAR(64) NOT NULL,
    n1_cot_justificativa        TEXT NOT NULL,
    n1_lauda_evidencia          INTEGER NOT NULL DEFAULT 0,
    n1_frase_citada             TEXT,
    n1_tipo_citacao             VARCHAR(32) NOT NULL,
    
    -- 8 Features Epistêmicas Ativas
    feat_fuzz_ratio             NUMERIC(5, 4) NOT NULL,
    feat_token_containment      NUMERIC(5, 4) NOT NULL,
    feat_exact_match            NUMERIC(3, 2) NOT NULL,
    feat_adversarial_density    NUMERIC(6, 4) NOT NULL,
    feat_sim_centroid_pred      NUMERIC(5, 4) NOT NULL,
    feat_mahalanobis_dist       NUMERIC(8, 4),
    feat_is_manuscrito          NUMERIC(3, 2) NOT NULL,
    feat_tesseract_mean_conf    NUMERIC(5, 4) NOT NULL,
    
    -- Sinais Estruturados Complementares
    auditoria_prefixo_auto      VARCHAR(20),
    auditoria_codigo_status     INTEGER,
    auditoria_fase_descricao    VARCHAR(64),
    auditoria_possui_espelho    BOOLEAN DEFAULT TRUE NOT NULL,
    
    -- Métricas de Calibração N=1 e Conformal CCCP
    probabilidade_calibrada_n1  NUMERIC(5, 4) NOT NULL CHECK (probabilidade_calibrada_n1 BETWEEN 0.0000 AND 1.0000),
    conjunto_conformal          VARCHAR(32)[] NOT NULL,
    tamanho_conjunto_conformal  SMALLINT NOT NULL,
    
    -- Segundo Estágio Adaptativo (N=2)
    estagio2_executado          BOOLEAN DEFAULT FALSE NOT NULL,
    n2_classe_predita           VARCHAR(32),
    n2_cot_justificativa        TEXT,
    n2_delta_cot_dispersao      NUMERIC(5, 4),
    n2_log_odds_atualizado      NUMERIC(6, 4),
    
    -- Decisão Operacional Final
    probabilidade_final         NUMERIC(5, 4) NOT NULL CHECK (probabilidade_final BETWEEN 0.0000 AND 1.0000),
    rota_execucao               VARCHAR(32) NOT NULL,
    faixa_operacional           VARCHAR(16) NOT NULL CHECK (faixa_operacional IN ('VERDE', 'AMARELA', 'VERMELHA')),
    
    -- Custos e Telemetria de Tokens (Auditoria de Context Caching)
    tokens_prompt_total         INTEGER DEFAULT 0 NOT NULL,
    tokens_completion           INTEGER DEFAULT 0 NOT NULL,
    tokens_cached               INTEGER DEFAULT 0 NOT NULL,
    custo_real_usd              NUMERIC(8, 6) DEFAULT 0.0 NOT NULL,
    custo_sem_cache_usd         NUMERIC(8, 6) DEFAULT 0.0 NOT NULL,
    economia_cache_usd          NUMERIC(8, 6) DEFAULT 0.0 NOT NULL,
    custo_estimado_usd          NUMERIC(8, 6) DEFAULT 0.0 NOT NULL,
    tempo_execucao_s            NUMERIC(6, 2) DEFAULT 0.0 NOT NULL,
    executado_em                TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_predicoes_processo 
ON predicoes_pipeline (processo_id);

CREATE INDEX IF NOT EXISTS idx_predicoes_faixa 
ON predicoes_pipeline (faixa_operacional);

CREATE INDEX IF NOT EXISTS idx_predicoes_pfinal 
ON predicoes_pipeline (probabilidade_final DESC);

CREATE INDEX IF NOT EXISTS idx_predicoes_calibracao 
ON predicoes_pipeline (calibracao_id);

-- TABELA
-- Fila de Controle de Lote e Idempotência da IA
CREATE TABLE IF NOT EXISTS fila_trabalho (
    fila_id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    processo_id             UUID NOT NULL REFERENCES processos_administrativos(processo_id) ON DELETE RESTRICT,
    status                  VARCHAR(32) DEFAULT 'PENDENTE_IA' NOT NULL 
                            CHECK (status IN (
                                'PENDENTE_IA', 
                                'PROCESSANDO_IA',
                                'CONCLUIDO_IA', 
                                'ERRO_PROCESSAMENTO'
                            )),
    tentativas              INTEGER DEFAULT 0 NOT NULL,
    ultima_mensagem_erro    TEXT,
    atualizado_em           TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fila_processo_status 
ON fila_trabalho (processo_id, status);

CREATE INDEX IF NOT EXISTS idx_fila_status 
ON fila_trabalho (status);

-- TABELA
-- Trilha de Auditoria Jurídica (Append-Only)
CREATE TABLE IF NOT EXISTS trilha_auditoria_juridica (
    auditoria_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    processo_id             UUID NOT NULL REFERENCES processos_administrativos(processo_id) ON DELETE RESTRICT,
    usuario_id              VARCHAR(64) NOT NULL,
    perfil_usuario          VARCHAR(32) NOT NULL,
    classe_anterior         VARCHAR(32) NOT NULL,
    classe_nova             VARCHAR(32) NOT NULL,
    fundamento_legal        TEXT NOT NULL,
    ip_origem               INET,
    hash_anterior           CHAR(64) NOT NULL,
    hash_registro           CHAR(64) NOT NULL,
    timestamp_registro      TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_auditoria_processo 
ON trilha_auditoria_juridica (processo_id, timestamp_registro DESC);

CREATE OR REPLACE FUNCTION proteger_trilha_auditoria()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'Operação não permitida: A tabela trilha_auditoria_juridica é estritamente append-only (vedado UPDATE ou DELETE).';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_bloquear_mutacao_auditoria ON trilha_auditoria_juridica;
CREATE TRIGGER trg_bloquear_mutacao_auditoria
BEFORE UPDATE OR DELETE ON trilha_auditoria_juridica
FOR EACH ROW EXECUTE FUNCTION proteger_trilha_auditoria();
