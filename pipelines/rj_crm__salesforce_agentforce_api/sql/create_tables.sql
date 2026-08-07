-- DDL das tabelas Agentforce no BigQuery
-- Dataset alvo    : rj-crm-registry.brutos_salesforce
-- Dataset controle: rj-crm-registry.agentforce_control
-- Particionamento : por data_particao (DATE)
-- Clustering      : por id (e chaves estrangeiras onde aplicável)
--
-- Colunas derivadas das queries reais do Data Cloud e CRM REST.
-- Nomes já normalizados pelo transform.py:
--   ssot__FooBar__c  →  foo_bar
--   FooBar__c        →  foo_bar   (sem prefixo)
--
-- Execute este script UMA vez antes de iniciar a pipeline.
-- Idempotente: todas as tabelas usam CREATE TABLE IF NOT EXISTS.
--
-- Campos removidos intencionalmente por causarem truncamento silencioso no DC:
--   ai_agent_interaction_step : input_value_text, output_value_text  → ai_agent_interaction_step_detail
--   conversation_entry        : payload_text                         → descartado (JSON bruto, irrelevante para dashboard)

-- ---------------------------------------------------------------------------
-- Pré-requisito: criar os datasets caso não existam
-- ---------------------------------------------------------------------------
-- CREATE SCHEMA IF NOT EXISTS `rj-crm-registry.brutos_salesforce`
--   OPTIONS (location = 'US');
-- CREATE SCHEMA IF NOT EXISTS `rj-crm-registry.agentforce_control`
--   OPTIONS (location = 'US');

-- ---------------------------------------------------------------------------
-- Fase 1 — STDM (Data Cloud DMOs: ssot__AiAgent*__dlm)
-- ---------------------------------------------------------------------------

-- Sessões do Agentforce (uma por atendimento iniciado via WhatsApp/canal)
CREATE TABLE IF NOT EXISTS `rj-crm-registry.brutos_salesforce.ai_agent_session`
(
    id                              STRING,
    related_messaging_session_id    STRING,
    ai_agent_channel_type           STRING,
    ai_agent_session_end_type       STRING,
    session_owner_id                STRING,
    session_owner_object            STRING,
    individual_id                   STRING,
    previous_session_id             STRING,
    start_timestamp                 TIMESTAMP,
    end_timestamp                   TIMESTAMP,
    internal_organization_id        STRING,
    data_source_id                  STRING,
    data_source_object_id           STRING,
    external_source_id              STRING,
    variable_text                   STRING,
    _loaded_at                      TIMESTAMP,
    data_particao                   DATE
)
PARTITION BY data_particao
CLUSTER BY id
OPTIONS (
    description = 'Sessoes do Agentforce — F1 STDM (AiAgentSession__dlm)',
    partition_expiration_days = 730
);

-- Interações dentro de uma sessão (uma por turno de conversa/tópico)
CREATE TABLE IF NOT EXISTS `rj-crm-registry.brutos_salesforce.ai_agent_interaction`
(
    id                              STRING,
    ai_agent_session_id             STRING,
    ai_agent_interaction_type       STRING,
    topic_api_name                  STRING,
    session_owner_id                STRING,
    session_owner_object            STRING,
    individual_id                   STRING,
    telemetry_trace_id              STRING,
    telemetry_trace_span_id         STRING,
    prev_interaction_id             STRING,
    start_timestamp                 TIMESTAMP,
    end_timestamp                   TIMESTAMP,
    internal_organization_id        STRING,
    data_source_id                  STRING,
    data_source_object_id           STRING,
    external_source_id              STRING,
    attribute_text                  STRING,
    _loaded_at                      TIMESTAMP,
    data_particao                   DATE
)
PARTITION BY data_particao
CLUSTER BY id, ai_agent_session_id
OPTIONS (
    description = 'Interacoes do Agentforce — F1 STDM (AiAgentInteraction__dlm)',
    partition_expiration_days = 730
);

-- Steps de cada interação (chamadas LLM, guardrails, ferramentas, etc.)
-- Campos pesados (input_value_text, output_value_text) estão em ai_agent_interaction_step_detail
CREATE TABLE IF NOT EXISTS `rj-crm-registry.brutos_salesforce.ai_agent_interaction_step`
(
    id                              STRING,
    ai_agent_interaction_id         STRING,
    ai_agent_interaction_step_type  STRING,
    name                            STRING,
    telemetry_trace_span_id         STRING,
    gen_ai_gateway_request_id       STRING,
    gen_ai_gateway_response_id      STRING,
    generation_id                   STRING,
    prev_step_id                    STRING,
    start_timestamp                 TIMESTAMP,
    end_timestamp                   TIMESTAMP,
    internal_organization_id        STRING,
    data_source_id                  STRING,
    data_source_object_id           STRING,
    external_source_id              STRING,
    pre_step_variable_text          STRING,
    post_step_variable_text         STRING,
    attribute_text                  STRING,
    error_message_text              STRING,
    sub_type                        STRING,
    _loaded_at                      TIMESTAMP,
    data_particao                   DATE
)
PARTITION BY data_particao
CLUSTER BY id, ai_agent_interaction_id
OPTIONS (
    description = 'Steps das interacoes do Agentforce — F1 STDM (AiAgentInteractionStep__dlm)',
    partition_expiration_days = 730
);

-- Campos pesados dos steps (input/output_value_text) — tabela separada para evitar truncamento DC
-- Contém apenas steps onde input_value_text existe e não é NOT_SET (LLM_STEPs relevantes)
CREATE TABLE IF NOT EXISTS `rj-crm-registry.brutos_salesforce.ai_agent_interaction_step_detail`
(
    id                              STRING,
    ai_agent_interaction_id         STRING,
    ai_agent_interaction_step_type  STRING,
    sub_type                        STRING,
    name                            STRING,
    input_value_text                STRING,
    output_value_text               STRING,
    start_timestamp                 TIMESTAMP,
    _loaded_at                      TIMESTAMP,
    data_particao                   DATE
)
PARTITION BY data_particao
CLUSTER BY id, ai_agent_interaction_id
OPTIONS (
    description = 'Campos pesados dos steps (input/output_value_text) — tabela separada para evitar truncamento DC',
    partition_expiration_days = 730
);

-- Mensagens trocadas em cada interação (input do usuário e output do agente)
CREATE TABLE IF NOT EXISTS `rj-crm-registry.brutos_salesforce.ai_agent_interaction_message`
(
    id                                      STRING,
    ai_agent_interaction_id                 STRING,
    ai_agent_session_id                     STRING,
    ai_agent_session_participant_id         STRING,
    ai_agent_interaction_message_type       STRING,
    ai_agent_interaction_msg_content_type   STRING,
    session_owner_id                        STRING,
    individual_id                           STRING,
    parent_message_id                       STRING,
    content_text                            STRING,
    message_sent_timestamp                  TIMESTAMP,
    message_start_timestamp                 TIMESTAMP,
    message_end_timestamp                   TIMESTAMP,
    internal_organization_id               STRING,
    data_source_id                          STRING,
    data_source_object_id                   STRING,
    external_source_id                      STRING,
    modality                                STRING,
    _loaded_at                              TIMESTAMP,
    data_particao                           DATE
)
PARTITION BY data_particao
CLUSTER BY id, ai_agent_interaction_id
OPTIONS (
    description = 'Mensagens das interacoes do Agentforce — F1 STDM (AiAgentInteractionMessage__dlm)',
    partition_expiration_days = 730
);

-- Entradas da conversa (eventos de canal: mensagens, typing, sistema, etc.)
-- payload_text removido intencionalmente (JSON bruto pesado, irrelevante para dashboard)
CREATE TABLE IF NOT EXISTS `rj-crm-registry.brutos_salesforce.conversation_entry`
(
    id                                  STRING,
    conversation_id                     STRING,
    conversation_entry_type             STRING,
    conversation_entry_visibility_type  STRING,
    engagement_participant_id           STRING,
    language                            STRING,
    duration_seconds_count              FLOAT64,
    version_number                      STRING,
    external_record_id                  STRING,
    client_date_time                    TIMESTAMP,
    transcripted_date_time              TIMESTAMP,
    created_date                        TIMESTAMP,
    last_modified_date                  TIMESTAMP,
    internal_organization_id            STRING,
    data_source_id                      STRING,
    data_source_object_id               STRING,
    kq_id                               STRING,
    _loaded_at                          TIMESTAMP,
    data_particao                       DATE
)
PARTITION BY data_particao
CLUSTER BY id
OPTIONS (
    description = 'Entradas da conversa — F1 STDM (ConversationEntry__dlm)',
    partition_expiration_days = 730
);

-- Spans de telemetria (rastreamento de latência por interação)
CREATE TABLE IF NOT EXISTS `rj-crm-registry.brutos_salesforce.telemetry_trace_span`
(
    id                              STRING,
    telemetry_trace                 STRING,
    telemetry_parent_span_id        STRING,
    operation_name                  STRING,
    span_kind                       STRING,
    start_date_time                 TIMESTAMP,
    end_date_time                   TIMESTAMP,
    duration_number                 FLOAT64,
    status_code                     STRING,
    service_name                    STRING,
    telemetry_span_attribute_text   STRING,
    data_source_id                  STRING,
    data_source_object_id           STRING,
    internal_organization_id        STRING,
    kq_id                           STRING,
    _loaded_at                      TIMESTAMP,
    data_particao                   DATE
)
PARTITION BY data_particao
CLUSTER BY id
OPTIONS (
    description = 'Spans de telemetria — F1 STDM (TelemetryTraceSpan__dlm)',
    partition_expiration_days = 730
);

-- ---------------------------------------------------------------------------
-- Fase 2 — CRM REST (objetos MessagingSession e MessagingEndUser)
-- ---------------------------------------------------------------------------

-- Sessões de mensageria (WhatsApp/canal — bridge entre Agentforce e usuário)
CREATE TABLE IF NOT EXISTS `rj-crm-registry.brutos_salesforce.messaging_session`
(
    id                      STRING,
    status                  STRING,
    start_time              TIMESTAMP,
    end_time                TIMESTAMP,
    messaging_channel_id    STRING,
    messaging_end_user_id   STRING,
    origin                  STRING,
    created_date            TIMESTAMP,
    last_modified_date      TIMESTAMP,
    _loaded_at              TIMESTAMP,
    data_particao           DATE
)
PARTITION BY data_particao
CLUSTER BY id
OPTIONS (
    description = 'Sessoes de mensageria — F2 CRM REST (MessagingSession)',
    partition_expiration_days = 730
);

-- Usuários finais (contatos que interagiram via WhatsApp/canal)
CREATE TABLE IF NOT EXISTS `rj-crm-registry.brutos_salesforce.messaging_end_user`
(
    id                          STRING,
    name                        STRING,
    messaging_channel_id        STRING,
    message_type                STRING,
    messaging_platform_key      STRING,
    locale                      STRING,
    iso_country_code            STRING,
    messaging_consent_status    STRING,
    is_fully_opted_in           BOOL,
    messaging_external_user_key STRING,
    created_date                TIMESTAMP,
    last_modified_date          TIMESTAMP,
    _loaded_at                  TIMESTAMP,
    data_particao               DATE
)
PARTITION BY data_particao
CLUSTER BY id
OPTIONS (
    description = 'Usuarios finais — F2 CRM REST (MessagingEndUser)',
    partition_expiration_days = 730
);

-- ---------------------------------------------------------------------------
-- Controle: watermarks de ingestão incremental
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS `rj-crm-registry.agentforce_control.pipeline_checkpoints`
(
    table_name      STRING NOT NULL,
    watermark       TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
)
OPTIONS (
    description = 'Watermarks de ingestao incremental da pipeline Agentforce'
);
