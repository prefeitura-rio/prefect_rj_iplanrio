# -*- coding: utf-8 -*-
"""
Criação automática das tabelas BigQuery da pipeline Agentforce.

Chamada no início de cada execução — idempotente (CREATE IF NOT EXISTS via SDK).
Cobre todas as fases: F1, F2a, F2b, F3, F4.
"""

from __future__ import annotations

from google.cloud import bigquery
from prefect import task

_TIMESTAMP = bigquery.enums.SqlTypeNames.TIMESTAMP
_STRING = bigquery.enums.SqlTypeNames.STRING
_DATE = bigquery.enums.SqlTypeNames.DATE
_FLOAT64 = bigquery.enums.SqlTypeNames.FLOAT64
_BOOL = bigquery.enums.SqlTypeNames.BOOLEAN


def _base_fields(extra: list[bigquery.SchemaField]) -> list[bigquery.SchemaField]:
    """Adiciona _loaded_at e data_particao ao final de qualquer schema."""
    return extra + [
        bigquery.SchemaField("_loaded_at", _TIMESTAMP),
        bigquery.SchemaField("data_particao", _DATE),
    ]


# ---------------------------------------------------------------------------
# Schemas por tabela
# ---------------------------------------------------------------------------

SCHEMAS: dict[str, list[bigquery.SchemaField]] = {
    # --- F1 — STDM ---
    "ai_agent_session": _base_fields([
        bigquery.SchemaField("id", _STRING),
        bigquery.SchemaField("related_messaging_session_id", _STRING),
        bigquery.SchemaField("ai_agent_channel_type", _STRING),
        bigquery.SchemaField("ai_agent_session_end_type", _STRING),
        bigquery.SchemaField("session_owner_id", _STRING),
        bigquery.SchemaField("session_owner_object", _STRING),
        bigquery.SchemaField("individual_id", _STRING),
        bigquery.SchemaField("previous_session_id", _STRING),
        bigquery.SchemaField("start_timestamp", _TIMESTAMP),
        bigquery.SchemaField("end_timestamp", _TIMESTAMP),
        bigquery.SchemaField("internal_organization_id", _STRING),
        bigquery.SchemaField("data_source_id", _STRING),
        bigquery.SchemaField("data_source_object_id", _STRING),
        bigquery.SchemaField("external_source_id", _STRING),
        bigquery.SchemaField("variable_text", _STRING),
    ]),
    "ai_agent_interaction": _base_fields([
        bigquery.SchemaField("id", _STRING),
        bigquery.SchemaField("ai_agent_session_id", _STRING),
        bigquery.SchemaField("ai_agent_interaction_type", _STRING),
        bigquery.SchemaField("topic_api_name", _STRING),
        bigquery.SchemaField("session_owner_id", _STRING),
        bigquery.SchemaField("session_owner_object", _STRING),
        bigquery.SchemaField("individual_id", _STRING),
        bigquery.SchemaField("telemetry_trace_id", _STRING),
        bigquery.SchemaField("telemetry_trace_span_id", _STRING),
        bigquery.SchemaField("prev_interaction_id", _STRING),
        bigquery.SchemaField("start_timestamp", _TIMESTAMP),
        bigquery.SchemaField("end_timestamp", _TIMESTAMP),
        bigquery.SchemaField("internal_organization_id", _STRING),
        bigquery.SchemaField("data_source_id", _STRING),
        bigquery.SchemaField("data_source_object_id", _STRING),
        bigquery.SchemaField("external_source_id", _STRING),
        bigquery.SchemaField("attribute_text", _STRING),
    ]),
    "ai_agent_interaction_step": _base_fields([
        bigquery.SchemaField("id", _STRING),
        bigquery.SchemaField("ai_agent_interaction_id", _STRING),
        bigquery.SchemaField("ai_agent_interaction_step_type", _STRING),
        bigquery.SchemaField("name", _STRING),
        bigquery.SchemaField("telemetry_trace_span_id", _STRING),
        bigquery.SchemaField("gen_ai_gateway_request_id", _STRING),
        bigquery.SchemaField("gen_ai_gateway_response_id", _STRING),
        bigquery.SchemaField("generation_id", _STRING),
        bigquery.SchemaField("prev_step_id", _STRING),
        bigquery.SchemaField("start_timestamp", _TIMESTAMP),
        bigquery.SchemaField("end_timestamp", _TIMESTAMP),
        bigquery.SchemaField("internal_organization_id", _STRING),
        bigquery.SchemaField("data_source_id", _STRING),
        bigquery.SchemaField("data_source_object_id", _STRING),
        bigquery.SchemaField("external_source_id", _STRING),
        bigquery.SchemaField("input_value_text", _STRING),
        bigquery.SchemaField("output_value_text", _STRING),
        bigquery.SchemaField("pre_step_variable_text", _STRING),
        bigquery.SchemaField("post_step_variable_text", _STRING),
        bigquery.SchemaField("attribute_text", _STRING),
        bigquery.SchemaField("error_message_text", _STRING),
        bigquery.SchemaField("sub_type", _STRING),
    ]),
    "ai_agent_interaction_message": _base_fields([
        bigquery.SchemaField("id", _STRING),
        bigquery.SchemaField("ai_agent_interaction_id", _STRING),
        bigquery.SchemaField("ai_agent_session_id", _STRING),
        bigquery.SchemaField("ai_agent_session_participant_id", _STRING),
        bigquery.SchemaField("ai_agent_interaction_message_type", _STRING),
        bigquery.SchemaField("ai_agent_interaction_msg_content_type", _STRING),
        bigquery.SchemaField("session_owner_id", _STRING),
        bigquery.SchemaField("individual_id", _STRING),
        bigquery.SchemaField("parent_message_id", _STRING),
        bigquery.SchemaField("content_text", _STRING),
        bigquery.SchemaField("message_sent_timestamp", _TIMESTAMP),
        bigquery.SchemaField("message_start_timestamp", _TIMESTAMP),
        bigquery.SchemaField("message_end_timestamp", _TIMESTAMP),
        bigquery.SchemaField("internal_organization_id", _STRING),
        bigquery.SchemaField("data_source_id", _STRING),
        bigquery.SchemaField("data_source_object_id", _STRING),
        bigquery.SchemaField("external_source_id", _STRING),
        bigquery.SchemaField("modality", _STRING),
    ]),

    # --- F2a — Messaging CRM ---
    "messaging_end_user": _base_fields([
        bigquery.SchemaField("id", _STRING),
        bigquery.SchemaField("name", _STRING),
        bigquery.SchemaField("messaging_channel_id", _STRING),
        bigquery.SchemaField("message_type", _STRING),
        bigquery.SchemaField("messaging_platform_key", _STRING),
        bigquery.SchemaField("locale", _STRING),
        bigquery.SchemaField("iso_country_code", _STRING),
        bigquery.SchemaField("messaging_consent_status", _STRING),
        bigquery.SchemaField("is_fully_opted_in", _BOOL),
        bigquery.SchemaField("messaging_external_user_key", _STRING),
        bigquery.SchemaField("language", _STRING),
        bigquery.SchemaField("created_date", _TIMESTAMP),
        bigquery.SchemaField("last_modified_date", _TIMESTAMP),
    ]),
    "messaging_session": _base_fields([
        bigquery.SchemaField("id", _STRING),
        bigquery.SchemaField("status", _STRING),
        bigquery.SchemaField("start_time", _TIMESTAMP),
        bigquery.SchemaField("end_time", _TIMESTAMP),
        bigquery.SchemaField("messaging_channel_id", _STRING),
        bigquery.SchemaField("messaging_end_user_id", _STRING),
        bigquery.SchemaField("origin", _STRING),
        bigquery.SchemaField("created_date", _TIMESTAMP),
        bigquery.SchemaField("last_modified_date", _TIMESTAMP),
    ]),
    "conversation_entry": _base_fields([
        bigquery.SchemaField("id", _STRING),
        bigquery.SchemaField("conversation_id", _STRING),
        bigquery.SchemaField("conversation_entry_type", _STRING),
        bigquery.SchemaField("conversation_entry_visibility_type", _STRING),
        bigquery.SchemaField("engagement_participant_id", _STRING),
        bigquery.SchemaField("payload_text", _STRING),
        bigquery.SchemaField("language", _STRING),
        bigquery.SchemaField("duration_seconds_count", _FLOAT64),
        bigquery.SchemaField("version_number", _STRING),
        bigquery.SchemaField("external_record_id", _STRING),
        bigquery.SchemaField("client_date_time", _TIMESTAMP),
        bigquery.SchemaField("transcripted_date_time", _TIMESTAMP),
        bigquery.SchemaField("created_date", _TIMESTAMP),
        bigquery.SchemaField("last_modified_date", _TIMESTAMP),
        bigquery.SchemaField("internal_organization_id", _STRING),
        bigquery.SchemaField("data_source_id", _STRING),
        bigquery.SchemaField("data_source_object_id", _STRING),
        bigquery.SchemaField("kq__id", _STRING),
    ]),

    # --- F3 — Platform Tracing ---
    "telemetry_trace_span": _base_fields([
        bigquery.SchemaField("id", _STRING),
        bigquery.SchemaField("telemetry_trace", _STRING),
        bigquery.SchemaField("telemetry_parent_span_id", _STRING),
        bigquery.SchemaField("operation_name", _STRING),
        bigquery.SchemaField("span_kind", _STRING),
        bigquery.SchemaField("start_date_time", _TIMESTAMP),
        bigquery.SchemaField("end_date_time", _TIMESTAMP),
        bigquery.SchemaField("duration_number", _FLOAT64),
        bigquery.SchemaField("status_code", _STRING),
        bigquery.SchemaField("service_name", _STRING),
        bigquery.SchemaField("telemetry_span_attribute_text", _STRING),
        bigquery.SchemaField("data_source_id", _STRING),
        bigquery.SchemaField("data_source_object_id", _STRING),
        bigquery.SchemaField("internal_organization_id", _STRING),
        bigquery.SchemaField("kq_id", _STRING),
    ]),
    "telemetry_trace_span_staging": _base_fields([
        bigquery.SchemaField("id", _STRING),
        bigquery.SchemaField("telemetry_trace", _STRING),
        bigquery.SchemaField("telemetry_parent_span_id", _STRING),
        bigquery.SchemaField("operation_name", _STRING),
        bigquery.SchemaField("span_kind", _STRING),
        bigquery.SchemaField("start_date_time", _TIMESTAMP),
        bigquery.SchemaField("end_date_time", _TIMESTAMP),
        bigquery.SchemaField("duration_number", _FLOAT64),
        bigquery.SchemaField("status_code", _STRING),
        bigquery.SchemaField("service_name", _STRING),
        bigquery.SchemaField("telemetry_span_attribute_text", _STRING),
        bigquery.SchemaField("data_source_id", _STRING),
        bigquery.SchemaField("data_source_object_id", _STRING),
        bigquery.SchemaField("internal_organization_id", _STRING),
        bigquery.SchemaField("kq_id", _STRING),
    ]),

}

# Tabelas com particionamento + clustering
PARTITIONED_TABLES: dict[str, list[str]] = {
    "ai_agent_session": ["id"],
    "ai_agent_interaction": ["id", "ai_agent_session_id"],
    "ai_agent_interaction_step": ["id", "ai_agent_interaction_id"],
    "ai_agent_interaction_message": ["id", "ai_agent_interaction_id"],
    "messaging_end_user": ["id"],
    "messaging_session": ["id"],
    "conversation_entry": ["id"],
    "telemetry_trace_span": ["id"],
    # telemetry_trace_span_staging: sem particionamento (staging table)
}


# ---------------------------------------------------------------------------
# Task
# ---------------------------------------------------------------------------


@task(log_prints=True)
def ensure_bq_tables(project_id: str, dataset_id: str) -> None:
    """
    Cria todas as tabelas da pipeline no BigQuery se ainda não existirem.
    Idempotente — seguro rodar a cada execução.

    Args:
        project_id : ID do projeto GCP.
        dataset_id : Dataset de destino.
    """
    client = bigquery.Client(project=project_id)
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)

    print(f"[ENSURE_TABLES] Verificando {len(SCHEMAS)} tabelas em '{project_id}.{dataset_id}'...")

    for table_id, schema in SCHEMAS.items():
        table_ref = dataset_ref.table(table_id)
        table = bigquery.Table(table_ref, schema=schema)

        if table_id in PARTITIONED_TABLES:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="data_particao",
            )
            table.clustering_fields = PARTITIONED_TABLES[table_id]

        client.create_table(table, exists_ok=True)
        print(f"[ENSURE_TABLES]   {table_id}: OK")

    print("[ENSURE_TABLES] Todas as tabelas verificadas.")
