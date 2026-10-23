-- ==============================================================================
-- SISTEMA DE TRIAGEM DE RECURSOS SMTR-RJ / IPLANRIO
-- BANCO DE DADOS: Cloud SQL (PostgreSQL 15+)
-- CONTRATO: PIPELINE PREFECT 3.0 <-> MCP SMTR (CLAUDE DESKTOP) <-> SEI SOAP v5.0
-- ==============================================================================

-- Extensões Criptográficas e Utilitárias
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ==============================================================================
-- 1. TABELA: papeis
-- Gerenciamento de Usuários, Matrículas, Perfis de Acesso e Unidades JARI
-- (Substitui a autenticação nominal no SEI durante a contingência da API SOAP)
-- ==============================================================================
CREATE TABLE IF NOT EXISTS papeis (
    usuario_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    usuario_sei            VARCHAR(64) NOT NULL UNIQUE,  -- Login no SEI (ex.: 'carlos.relator')
    matricula              VARCHAR(32) NOT NULL UNIQUE,  -- Matrícula funcional SMTR / IPLANRIO
    email                  VARCHAR(128) NOT NULL UNIQUE, -- E-mail corporativo (Claude Enterprise SSO)
    nome                   VARCHAR(128) NOT NULL,
    papel                  VARCHAR(20) NOT NULL CHECK (papel IN ('JARISTA', 'GESTOR', 'ADMIN')),
    codigo_jari            VARCHAR(20) NOT NULL,         -- Unidade JARI (ex.: '1_JARI')
    unidade_sei_id         VARCHAR(32),                  -- ID numérico da unidade no SEI (ex.: '110000001')
    ativo                  BOOLEAN DEFAULT TRUE NOT NULL,
    criado_em              TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_papeis_email ON papeis (email);
CREATE INDEX IF NOT EXISTS idx_papeis_matricula ON papeis (matricula);
CREATE INDEX IF NOT EXISTS idx_papeis_jari_papel ON papeis (codigo_jari, papel);

-- ==============================================================================
-- 2. TABELA: processos_administrativos
-- Identificação, Ingestão e Atribuição de Processos no SEI
-- ==============================================================================
CREATE TABLE IF NOT EXISTS processos_administrativos (
    processo_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    numero_processo_sei     VARCHAR(32) NOT NULL UNIQUE,
    id_procedimento_sei     VARCHAR(32),                 -- ID interno do processo no SEI
    id_documento_sei        VARCHAR(64),                 -- ID interno do documento principal de recurso no SEI
    numero_auto_infracao    VARCHAR(20),
    data_autuacao           DATE,
    origem_lote             VARCHAR(20) NOT NULL CHECK (origem_lote IN ('GOLDENSET', 'PASSIVO', 'MENSAL', 'SEI_JARI')),
    nome_arquivo_origem     VARCHAR(255),
    hash_sha256_documento   CHAR(64),
    
    -- Metadados de Unidade JARI e Atribuição Nominal
    codigo_jari             VARCHAR(20) DEFAULT '1_JARI' NOT NULL,
    unidade_sei_id          VARCHAR(32),
    atribuido_a_login_sei   VARCHAR(64),                 -- Login/Matrícula do Jarista relator responsável
    status_distribuicao     VARCHAR(20) DEFAULT 'PENDENTE' NOT NULL 
                            CHECK (status_distribuicao IN ('PENDENTE', 'DISTRIBUIDO', 'CONCLUIDO')),
    data_distribuicao       TIMESTAMPTZ,
    
    criado_em               TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_proc_data_autuacao 
ON processos_administrativos (data_autuacao);

CREATE INDEX IF NOT EXISTS idx_proc_origem 
ON processos_administrativos (origem_lote);

CREATE INDEX IF NOT EXISTS idx_proc_doc_sei 
ON processos_administrativos (id_documento_sei);

CREATE INDEX IF NOT EXISTS idx_proc_jari 
ON processos_administrativos (codigo_jari);

CREATE INDEX IF NOT EXISTS idx_proc_atribuicao 
ON processos_administrativos (atribuido_a_login_sei, status_distribuicao);

-- ==============================================================================
-- 3. TABELA: predicoes_pipeline
-- Predições da Pipeline de IA, Features Epistêmicas e Calibração Conformal
-- ==============================================================================
CREATE TABLE IF NOT EXISTS predicoes_pipeline (
    predicao_id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    processo_id                 UUID NOT NULL REFERENCES processos_administrativos(processo_id) ON DELETE RESTRICT,
    calibracao_id               UUID,
    modelo_llm                  VARCHAR(64) DEFAULT 'vertex/gemini-3.7-flash' NOT NULL,
    versao_prompt               VARCHAR(64) DEFAULT 'v4_baseline' NOT NULL,
    id_documento_sei            VARCHAR(64),
    numero_processo_sei         VARCHAR(32),
    codigo_jari                 VARCHAR(20),
    vigente                     BOOLEAN DEFAULT TRUE NOT NULL,
    
    -- Saída Principal da IA (N=1)
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
    data_fato_infracao          DATE,
    data_expedicao_penalidade   DATE,
    
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

CREATE INDEX IF NOT EXISTS idx_predicoes_vigente 
ON predicoes_pipeline (processo_id, vigente);

-- ==============================================================================
-- 4. TABELA: fila_trabalho
-- Fila de Controle de Lote e Idempotência da Pipeline de IA
-- ==============================================================================
CREATE TABLE IF NOT EXISTS fila_trabalho (
    fila_id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    processo_id             UUID NOT NULL REFERENCES processos_administrativos(processo_id) ON DELETE RESTRICT,
    status                  VARCHAR(32) DEFAULT 'PENDENTE_IA' NOT NULL 
                            CHECK (status IN (
                                'PENDENTE_IA', 
                                'PROCESSANDO_IA',
                                'CONCLUIDO_IA', 
                                'ERRO_PROCESSAMENTO',
                                'IGNORADO'
                            )),
    tentativas              INTEGER DEFAULT 0 NOT NULL,
    ultima_mensagem_erro    TEXT,
    atualizado_em           TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fila_processo_status 
ON fila_trabalho (processo_id, status);

CREATE INDEX IF NOT EXISTS idx_fila_status 
ON fila_trabalho (status);

-- ==============================================================================
-- 5. TABELA: revisoes
-- Histórico Aditivo Imutável de Decisões Humanas do Jarista (Claude MCP)
-- ==============================================================================
CREATE TABLE IF NOT EXISTS revisoes (
    revisao_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    processo_id            UUID NOT NULL REFERENCES processos_administrativos(processo_id) ON DELETE RESTRICT,
    predicao_id            UUID NOT NULL REFERENCES predicoes_pipeline(predicao_id) ON DELETE RESTRICT,
    usuario_sei            VARCHAR(64) NOT NULL,
    matricula_jarista      VARCHAR(32),
    recomendacao_exibida   BOOLEAN NOT NULL,
    concordou              BOOLEAN NOT NULL,
    classe_final           VARCHAR(32) NOT NULL,
    encaminhamento_final   VARCHAR(64) NOT NULL,
    fundamento             TEXT, -- Obrigatório se discordou da IA ou em análise manual às cegas
    criado_em              TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_revisoes_processo 
ON revisoes (processo_id);

CREATE INDEX IF NOT EXISTS idx_revisoes_predicao 
ON revisoes (predicao_id);

CREATE INDEX IF NOT EXISTS idx_revisoes_usuario 
ON revisoes (usuario_sei, criado_em DESC);

-- ==============================================================================
-- 6. TABELA: eventos
-- Ciclo de Vida do Processo, Tempos Operacionais e Auditoria de Acesso
-- ==============================================================================
CREATE TABLE IF NOT EXISTS eventos (
    evento_id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    processo_id            UUID NOT NULL REFERENCES processos_administrativos(processo_id) ON DELETE RESTRICT,
    usuario_sei            VARCHAR(64),
    tipo_evento            VARCHAR(32) NOT NULL CHECK (tipo_evento IN (
                                'INGESTAO_SEI', 
                                'TRIADO_IA', 
                                'LISTOU_RECURSOS', 
                                'VIU_ANALISE', 
                                'VIU_DOCUMENTO', 
                                'DISTRIBUIDO', 
                                'REDISTRIBUIDO', 
                                'REVISADO', 
                                'BLOCO_CRIADO', 
                                'HOMOLOGADO', 
                                'ERRO'
                            )),
    detalhes_json          JSONB,
    registrado_em          TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_eventos_processo 
ON eventos (processo_id, registrado_em DESC);

CREATE INDEX IF NOT EXISTS idx_eventos_tipo 
ON eventos (tipo_evento);

-- ==============================================================================
-- 7. TABELA: trilha_auditoria_juridica
-- Trilha Legal Criptográfica Imutável (Append-Only)
-- ==============================================================================
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

-- ==============================================================================
-- 8. VIEW: predicao_atual
-- Interface Otimizada de Consumo em Tempo Real para o MCP SMTR (Claude Desktop)
-- ==============================================================================
CREATE OR REPLACE VIEW predicao_atual AS
SELECT 
    p.predicao_id,
    p.processo_id,
    proc.numero_processo_sei,
    proc.id_procedimento_sei,
    proc.id_documento_sei,
    proc.codigo_jari,
    proc.atribuido_a_login_sei,
    proc.status_distribuicao,
    p.n1_classe_predita,
    p.n1_encaminhamento,
    p.n1_cot_justificativa,
    p.n1_lauda_evidencia,
    p.n1_frase_citada,
    p.probabilidade_final,
    p.faixa_operacional,
    p.rota_execucao,
    p.tokens_cached,
    p.custo_real_usd,
    p.executado_em
FROM predicoes_pipeline p
JOIN processos_administrativos proc ON proc.processo_id = p.processo_id
WHERE p.vigente = TRUE;
