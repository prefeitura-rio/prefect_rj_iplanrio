-- DDL das tabelas Agentforce no BigQuery
-- Dataset alvo  : rj-crm-registry.brutos_salesforce
-- Dataset controle: rj-crm-registry.agentforce_control
-- Particionamento: por data_particao (DATE)
-- Clustering    : por id (e chaves estrangeiras onde aplicável)
--
-- Colunas derivadas das queries reais do Data Cloud (2026-07-27).
-- Nomes já normalizados pelo transform.py:
--   ssot__FooBar__c  →  foo_bar
--   FooBar__c        →  foo_bar   (sem prefixo)
--
-- Execute este script UMA vez antes de iniciar a pipeline.
-- Idempotente: todas as tabelas usam CREATE TABLE IF NOT EXISTS.

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
    input_value_text                STRING,
    output_value_text               STRING,
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
