-- DDL das tabelas Agentforce no BigQuery
-- Dataset: brutos_salesforce_crm
-- Particionamento: por data_particao (DATE)
-- Clustering: por id (onde aplicável)
--
-- Execute este script UMA vez antes de iniciar a pipeline.
-- As tabelas são criadas com IF NOT EXISTS para ser idempotente.

-- ---------------------------------------------------------------------------
-- Fase 1 — STDM
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS `brutos_salesforce_crm.ai_agent_session`
(
    id                   STRING,
    name                 STRING,
    agent_type           STRING,
    start_time           TIMESTAMP,
    end_time             TIMESTAMP,
    status               STRING,
    channel_type         STRING,
    owner_id             STRING,
    created_date         TIMESTAMP,
    last_modified_date   TIMESTAMP,
    _loaded_at           TIMESTAMP,
    data_particao        DATE
)
PARTITION BY data_particao
CLUSTER BY id
OPTIONS (
    description = 'Sessoes do Agentforce — F1 STDM',
    partition_expiration_days = 730
);

CREATE TABLE IF NOT EXISTS `brutos_salesforce_crm.ai_agent_interaction`
(
    id                   STRING,
    session_id           STRING,
    interaction_type     STRING,
    start_time           TIMESTAMP,
    end_time             TIMESTAMP,
    status               STRING,
    created_date         TIMESTAMP,
    _loaded_at           TIMESTAMP,
    data_particao        DATE
)
PARTITION BY data_particao
CLUSTER BY id, session_id
OPTIONS (
    description = 'Interacoes do Agentforce — F1 STDM',
    partition_expiration_days = 730
);

CREATE TABLE IF NOT EXISTS `brutos_salesforce_crm.ai_agent_interaction_step`
(
    id                   STRING,
    interaction_id       STRING,
    step_type            STRING,
    step_name            STRING,
    start_time           TIMESTAMP,
    duration_ms          FLOAT64,
    status               STRING,
    created_date         TIMESTAMP,
    _loaded_at           TIMESTAMP,
    data_particao        DATE
)
PARTITION BY data_particao
CLUSTER BY id, interaction_id
OPTIONS (
    description = 'Steps das interacoes do Agentforce — F1 STDM',
    partition_expiration_days = 730
);

CREATE TABLE IF NOT EXISTS `brutos_salesforce_crm.ai_agent_interaction_message`
(
    id                   STRING,
    interaction_id       STRING,
    message_type         STRING,
    content              STRING,
    created_date         TIMESTAMP,
    _loaded_at           TIMESTAMP,
    data_particao        DATE
)
PARTITION BY data_particao
CLUSTER BY id, interaction_id
OPTIONS (
    description = 'Mensagens das interacoes do Agentforce — F1 STDM',
    partition_expiration_days = 730
);

CREATE TABLE IF NOT EXISTS `brutos_salesforce_crm.ai_agent_interaction_participant`
(
    id                   STRING,
    interaction_id       STRING,
    participant_type     STRING,
    participant_id       STRING,
    created_date         TIMESTAMP,
    _loaded_at           TIMESTAMP,
    data_particao        DATE
)
PARTITION BY data_particao
CLUSTER BY id, interaction_id
OPTIONS (
    description = 'Participantes das interacoes do Agentforce — F1 STDM',
    partition_expiration_days = 730
);

-- ---------------------------------------------------------------------------
-- Fase 2a — Messaging
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS `brutos_salesforce_crm.messaging_session`
(
    id                   STRING,
    status               STRING,
    start_time           TIMESTAMP,
    end_time             TIMESTAMP,
    messaging_channel    STRING,
    origin               STRING,
    created_date         TIMESTAMP,
    last_modified_date   TIMESTAMP,
    _loaded_at           TIMESTAMP,
    data_particao        DATE
)
PARTITION BY data_particao
CLUSTER BY id
OPTIONS (
    description = 'Sessoes de mensageria (WhatsApp, SMS, etc.) — F2a',
    partition_expiration_days = 730
);

CREATE TABLE IF NOT EXISTS `brutos_salesforce_crm.conversation_entry`
(
    id                   STRING,
    conversation_id      STRING,
    actor_type           STRING,
    entry_type           STRING,
    message              STRING,
    entry_payload        JSON,
    created_date         TIMESTAMP,
    _loaded_at           TIMESTAMP,
    data_particao        DATE
)
PARTITION BY data_particao
CLUSTER BY id, conversation_id
OPTIONS (
    description = 'Entradas detalhadas de conversa — F2a',
    partition_expiration_days = 730
);

-- ---------------------------------------------------------------------------
-- Fase 3 — Platform Tracing
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS `brutos_salesforce_crm.telemetry_trace_span`
(
    id                   STRING,
    trace_id             STRING,
    span_id              STRING,
    parent_span_id       STRING,
    span_name            STRING,
    span_kind            STRING,
    start_time_ms        FLOAT64,
    end_time_ms          FLOAT64,
    duration_ms          FLOAT64,
    status               STRING,
    service_name         STRING,
    created_date         TIMESTAMP,
    _loaded_at           TIMESTAMP,
    data_particao        DATE
)
PARTITION BY data_particao
CLUSTER BY trace_id, span_id
OPTIONS (
    description = 'Telemetria de tracing do Agentforce — F3',
    partition_expiration_days = 365
);

CREATE TABLE IF NOT EXISTS `brutos_salesforce_crm.telemetry_trace_span_staging`
(
    id                   STRING,
    trace_id             STRING,
    span_id              STRING,
    parent_span_id       STRING,
    span_name            STRING,
    span_kind            STRING,
    start_time_ms        FLOAT64,
    end_time_ms          FLOAT64,
    duration_ms          FLOAT64,
    status               STRING,
    service_name         STRING,
    created_date         TIMESTAMP,
    _loaded_at           TIMESTAMP,
    data_particao        DATE
)
OPTIONS (
    description = 'Staging para carga chunked dos spans — limpa apos MERGE'
);

-- ---------------------------------------------------------------------------
-- Fase 4 — GenAI Audit
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS `brutos_salesforce_crm.genai_gateway_request`
(
    id                   STRING,
    model                STRING,
    request_tokens       INT64,
    response_tokens      INT64,
    latency_ms           FLOAT64,
    status               STRING,
    created_date         TIMESTAMP,
    _loaded_at           TIMESTAMP,
    data_particao        DATE
)
PARTITION BY data_particao
CLUSTER BY id
OPTIONS (
    description = 'Requisicoes ao gateway GenAI — F4',
    partition_expiration_days = 730
);

CREATE TABLE IF NOT EXISTS `brutos_salesforce_crm.genai_generation`
(
    id                   STRING,
    request_id           STRING,
    generated_text       STRING,
    finish_reason        STRING,
    created_date         TIMESTAMP,
    _loaded_at           TIMESTAMP,
    data_particao        DATE
)
PARTITION BY data_particao
CLUSTER BY id, request_id
OPTIONS (
    description = 'Geracoes do modelo GenAI — F4',
    partition_expiration_days = 730
);

CREATE TABLE IF NOT EXISTS `brutos_salesforce_crm.genai_quality`
(
    id                   STRING,
    generation_id        STRING,
    quality_score        FLOAT64,
    quality_type         STRING,
    created_date         TIMESTAMP,
    _loaded_at           TIMESTAMP,
    data_particao        DATE
)
PARTITION BY data_particao
OPTIONS (
    description = 'Metricas de qualidade do GenAI — F4',
    partition_expiration_days = 730
);

CREATE TABLE IF NOT EXISTS `brutos_salesforce_crm.genai_feedback`
(
    id                   STRING,
    generation_id        STRING,
    feedback_type        STRING,
    rating               INT64,
    comment              STRING,
    created_date         TIMESTAMP,
    _loaded_at           TIMESTAMP,
    data_particao        DATE
)
PARTITION BY data_particao
OPTIONS (
    description = 'Feedbacks sobre respostas do GenAI — F4',
    partition_expiration_days = 730
);

-- ---------------------------------------------------------------------------
-- Controle: watermarks
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS `agentforce_control.pipeline_checkpoints`
(
    table_name           STRING NOT NULL,
    watermark            TIMESTAMP NOT NULL,
    updated_at           TIMESTAMP NOT NULL
)
OPTIONS (
    description = 'Watermarks de ingestao incremental da pipeline Agentforce'
);
