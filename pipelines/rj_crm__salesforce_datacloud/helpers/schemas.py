"""BigQuery schemas of the tables of the pipeline, final and staging, as data.

`create_tables` creates them on each run, and `load_staging` uses them for the load.
"""

from google.cloud import bigquery

TIMESTAMP = bigquery.enums.SqlTypeNames.TIMESTAMP
STRING = bigquery.enums.SqlTypeNames.STRING
DATE = bigquery.enums.SqlTypeNames.DATE
FLOAT64 = bigquery.enums.SqlTypeNames.FLOAT64
BOOL = bigquery.enums.SqlTypeNames.BOOLEAN


def add_base_fields(extra: list[bigquery.SchemaField]) -> list[bigquery.SchemaField]:
    """Return the schema with `_loaded_at` and `data_particao` at the end."""
    return [*extra, bigquery.SchemaField("_loaded_at", TIMESTAMP), bigquery.SchemaField("data_particao", DATE)]


# --- Eventos WhatsApp (DLL MessagingEventsWhatsAppV2_00Das_4CAB1BC2__dll) ---
# Nomes = saída de _clean_dc_field_name (transform.py) sobre as colunas de
# sql/messaging_events_whatsapp.sql. Chave do MERGE: event_id (EventId__c —
# conferido único e nunca vazio no histórico todo em 2026-09-29).
MESSAGING_EVENTS_WHATSAPP_FIELDS = [
    bigquery.SchemaField("event_id", STRING),
    bigquery.SchemaField("event_date_time", TIMESTAMP),
    bigquery.SchemaField("event_type", STRING),
    bigquery.SchemaField("send_status", STRING),
    bigquery.SchemaField("not_sent_reason", STRING),
    bigquery.SchemaField("error_code", STRING),
    bigquery.SchemaField("contact_point_phone_number", STRING),
    bigquery.SchemaField("individual_id", STRING),
    bigquery.SchemaField("kq_individual_id", STRING),
    bigquery.SchemaField("whats_app_wam_id", STRING),
    bigquery.SchemaField("message_id", STRING),
    bigquery.SchemaField("bulk_message_id", STRING),
    bigquery.SchemaField("content_id", STRING),
    bigquery.SchemaField("message", STRING),
    bigquery.SchemaField("message_purpose", STRING),
    bigquery.SchemaField("pricing_category", STRING),
    bigquery.SchemaField("journey_id", STRING),
    bigquery.SchemaField("flow_element_run_id", STRING),
    bigquery.SchemaField("kq_flow_element_run_id", STRING),
    bigquery.SchemaField("kq_event_id", STRING),
    bigquery.SchemaField("engagement_channel", STRING),
    bigquery.SchemaField("kq_engagement_channel", STRING),
    bigquery.SchemaField("application", STRING),
    bigquery.SchemaField("source", STRING),
    bigquery.SchemaField("sender_id", STRING),
    bigquery.SchemaField("sender_display_name", STRING),
    bigquery.SchemaField("username", STRING),
    bigquery.SchemaField("business_unit_id", STRING),
    bigquery.SchemaField("external_account_id", STRING),
    bigquery.SchemaField("tenant", STRING),
    bigquery.SchemaField("bsuid", STRING),
    bigquery.SchemaField("parent_bsuid", STRING),
    bigquery.SchemaField("ctwa_campaign_source_type", STRING),
    bigquery.SchemaField("ctwa_campaign_source_id", STRING),
    bigquery.SchemaField("ctwa_click_id", STRING),
    bigquery.SchemaField("link_url", STRING),
    bigquery.SchemaField("resolved_url", STRING),
    bigquery.SchemaField("user_agent", STRING),
    bigquery.SchemaField("global_event", BOOL),
    bigquery.SchemaField("data_source", STRING),
    bigquery.SchemaField("data_source_object", STRING),
    bigquery.SchemaField("cdp_sys_source_version", STRING),
    bigquery.SchemaField("cdp_sys_partition_date", TIMESTAMP),
]

# --- Send events WhatsApp do SFMC (DLL sfmc_whatsapp_send_event_534019838__dll) ---
# Nomes = saída de _clean_dc_field_name sobre as colunas de
# sql/sfmc_whatsapp_send_event.sql. Chave do MERGE: id (ID__c — conferido
# único e nunca vazio no histórico todo em 2026-10-02).
SFMC_WHATSAPP_SEND_EVENT_FIELDS = [
    bigquery.SchemaField("id", STRING),
    bigquery.SchemaField("engagement_date_time", TIMESTAMP),
    bigquery.SchemaField("accepted_time_utc", TIMESTAMP),
    bigquery.SchemaField("last_modified_date", TIMESTAMP),
    bigquery.SchemaField("engagement_channel_action", STRING),
    bigquery.SchemaField("message_recipient_send_status", STRING),
    bigquery.SchemaField("event_direction", STRING),
    bigquery.SchemaField("reason", STRING),
    bigquery.SchemaField("engagement_action_reason", STRING),
    bigquery.SchemaField("engagement_notes_txt", STRING),
    bigquery.SchemaField("message_key", STRING),
    bigquery.SchemaField("bulk_message_id", STRING),
    bigquery.SchemaField("kq_bulk_message_id", STRING),
    bigquery.SchemaField("journey_id", STRING),
    bigquery.SchemaField("journey_activity_id", STRING),
    bigquery.SchemaField("asset_id", STRING),
    bigquery.SchemaField("message_text", STRING),
    bigquery.SchemaField("mobile_number", STRING),
    bigquery.SchemaField("country_code", STRING),
    bigquery.SchemaField("channel_id", STRING),
    bigquery.SchemaField("whats_app_id", STRING),
    bigquery.SchemaField("bsuid", STRING),
    bigquery.SchemaField("subscriber_key", STRING),
    bigquery.SchemaField("kq_subscriber_key", STRING),
    bigquery.SchemaField("subscriber_id", STRING),
    bigquery.SchemaField("contact_point_id", STRING),
    bigquery.SchemaField("kq_contact_point_id", STRING),
    bigquery.SchemaField("engagement_channel_type", STRING),
    bigquery.SchemaField("kq_engagement_channel_type", STRING),
    bigquery.SchemaField("omni_post_model_type_id", FLOAT64),
    bigquery.SchemaField("app_id", STRING),
    bigquery.SchemaField("business_manager_id", STRING),
    bigquery.SchemaField("internal_organization", STRING),
    bigquery.SchemaField("sender_display_name", STRING),
    bigquery.SchemaField("data_source", STRING),
    bigquery.SchemaField("data_source_object", STRING),
    bigquery.SchemaField("kq_id", STRING),
    bigquery.SchemaField("cdp_sys_source_version", STRING),
    bigquery.SchemaField("cdp_sys_partition_date", TIMESTAMP),
]

# ---------------------------------------------------------------------------
# Schemas por tabela
# ---------------------------------------------------------------------------

SCHEMAS: dict[str, list[bigquery.SchemaField]] = {
    # --- Agentforce (STDM) ---
    "ai_agent_session": add_base_fields(
        [
            bigquery.SchemaField("id", STRING),
            bigquery.SchemaField("related_messaging_session_id", STRING),
            bigquery.SchemaField("ai_agent_channel_type", STRING),
            bigquery.SchemaField("ai_agent_session_end_type", STRING),
            bigquery.SchemaField("session_owner_id", STRING),
            bigquery.SchemaField("session_owner_object", STRING),
            bigquery.SchemaField("individual_id", STRING),
            bigquery.SchemaField("previous_session_id", STRING),
            bigquery.SchemaField("start_timestamp", TIMESTAMP),
            bigquery.SchemaField("end_timestamp", TIMESTAMP),
            bigquery.SchemaField("internal_organization_id", STRING),
            bigquery.SchemaField("data_source_id", STRING),
            bigquery.SchemaField("data_source_object_id", STRING),
            bigquery.SchemaField("external_source_id", STRING),
            bigquery.SchemaField("variable_text", STRING),
        ]
    ),
    "ai_agent_interaction": add_base_fields(
        [
            bigquery.SchemaField("id", STRING),
            bigquery.SchemaField("ai_agent_session_id", STRING),
            bigquery.SchemaField("ai_agent_interaction_type", STRING),
            bigquery.SchemaField("topic_api_name", STRING),
            bigquery.SchemaField("session_owner_id", STRING),
            bigquery.SchemaField("session_owner_object", STRING),
            bigquery.SchemaField("individual_id", STRING),
            bigquery.SchemaField("telemetry_trace_id", STRING),
            bigquery.SchemaField("telemetry_trace_span_id", STRING),
            bigquery.SchemaField("prev_interaction_id", STRING),
            bigquery.SchemaField("start_timestamp", TIMESTAMP),
            bigquery.SchemaField("end_timestamp", TIMESTAMP),
            bigquery.SchemaField("internal_organization_id", STRING),
            bigquery.SchemaField("data_source_id", STRING),
            bigquery.SchemaField("data_source_object_id", STRING),
            bigquery.SchemaField("external_source_id", STRING),
            bigquery.SchemaField("attribute_text", STRING),
        ]
    ),
    "ai_agent_interaction_step": add_base_fields(
        [
            bigquery.SchemaField("id", STRING),
            bigquery.SchemaField("ai_agent_interaction_id", STRING),
            bigquery.SchemaField("ai_agent_interaction_step_type", STRING),
            bigquery.SchemaField("name", STRING),
            bigquery.SchemaField("telemetry_trace_span_id", STRING),
            bigquery.SchemaField("gen_ai_gateway_request_id", STRING),
            bigquery.SchemaField("gen_ai_gateway_response_id", STRING),
            bigquery.SchemaField("generation_id", STRING),
            bigquery.SchemaField("prev_step_id", STRING),
            bigquery.SchemaField("start_timestamp", TIMESTAMP),
            bigquery.SchemaField("end_timestamp", TIMESTAMP),
            bigquery.SchemaField("internal_organization_id", STRING),
            bigquery.SchemaField("data_source_id", STRING),
            bigquery.SchemaField("data_source_object_id", STRING),
            bigquery.SchemaField("external_source_id", STRING),
            bigquery.SchemaField("pre_step_variable_text", STRING),
            bigquery.SchemaField("post_step_variable_text", STRING),
            bigquery.SchemaField("attribute_text", STRING),
            bigquery.SchemaField("error_message_text", STRING),
            bigquery.SchemaField("sub_type", STRING),
            bigquery.SchemaField("output_value_text", STRING),
        ]
    ),
    "ai_agent_interaction_message": add_base_fields(
        [
            bigquery.SchemaField("id", STRING),
            bigquery.SchemaField("ai_agent_interaction_id", STRING),
            bigquery.SchemaField("ai_agent_session_id", STRING),
            bigquery.SchemaField("ai_agent_session_participant_id", STRING),
            bigquery.SchemaField("ai_agent_interaction_message_type", STRING),
            bigquery.SchemaField("ai_agent_interaction_msg_content_type", STRING),
            bigquery.SchemaField("session_owner_id", STRING),
            bigquery.SchemaField("individual_id", STRING),
            bigquery.SchemaField("parent_message_id", STRING),
            bigquery.SchemaField("content_text", STRING),
            bigquery.SchemaField("message_sent_timestamp", TIMESTAMP),
            bigquery.SchemaField("message_start_timestamp", TIMESTAMP),
            bigquery.SchemaField("message_end_timestamp", TIMESTAMP),
            bigquery.SchemaField("internal_organization_id", STRING),
            bigquery.SchemaField("data_source_id", STRING),
            bigquery.SchemaField("data_source_object_id", STRING),
            bigquery.SchemaField("external_source_id", STRING),
            bigquery.SchemaField("modality", STRING),
        ]
    ),
    # Staging das 4 acima — desde 2026-09-08, write_mode='merge' via modo janela
    # (ver utils/janela.py e flow.py): a janela rolante de 1h recaptura
    # o mesmo registro de propósito a cada tick, então precisa de staging+MERGE
    # em vez de append puro (que duplicaria a cada sobreposição).
    "ai_agent_session_staging": add_base_fields(
        [
            bigquery.SchemaField("id", STRING),
            bigquery.SchemaField("related_messaging_session_id", STRING),
            bigquery.SchemaField("ai_agent_channel_type", STRING),
            bigquery.SchemaField("ai_agent_session_end_type", STRING),
            bigquery.SchemaField("session_owner_id", STRING),
            bigquery.SchemaField("session_owner_object", STRING),
            bigquery.SchemaField("individual_id", STRING),
            bigquery.SchemaField("previous_session_id", STRING),
            bigquery.SchemaField("start_timestamp", TIMESTAMP),
            bigquery.SchemaField("end_timestamp", TIMESTAMP),
            bigquery.SchemaField("internal_organization_id", STRING),
            bigquery.SchemaField("data_source_id", STRING),
            bigquery.SchemaField("data_source_object_id", STRING),
            bigquery.SchemaField("external_source_id", STRING),
            bigquery.SchemaField("variable_text", STRING),
        ]
    ),
    "ai_agent_interaction_staging": add_base_fields(
        [
            bigquery.SchemaField("id", STRING),
            bigquery.SchemaField("ai_agent_session_id", STRING),
            bigquery.SchemaField("ai_agent_interaction_type", STRING),
            bigquery.SchemaField("topic_api_name", STRING),
            bigquery.SchemaField("session_owner_id", STRING),
            bigquery.SchemaField("session_owner_object", STRING),
            bigquery.SchemaField("individual_id", STRING),
            bigquery.SchemaField("telemetry_trace_id", STRING),
            bigquery.SchemaField("telemetry_trace_span_id", STRING),
            bigquery.SchemaField("prev_interaction_id", STRING),
            bigquery.SchemaField("start_timestamp", TIMESTAMP),
            bigquery.SchemaField("end_timestamp", TIMESTAMP),
            bigquery.SchemaField("internal_organization_id", STRING),
            bigquery.SchemaField("data_source_id", STRING),
            bigquery.SchemaField("data_source_object_id", STRING),
            bigquery.SchemaField("external_source_id", STRING),
            bigquery.SchemaField("attribute_text", STRING),
        ]
    ),
    "ai_agent_interaction_step_staging": add_base_fields(
        [
            bigquery.SchemaField("id", STRING),
            bigquery.SchemaField("ai_agent_interaction_id", STRING),
            bigquery.SchemaField("ai_agent_interaction_step_type", STRING),
            bigquery.SchemaField("name", STRING),
            bigquery.SchemaField("telemetry_trace_span_id", STRING),
            bigquery.SchemaField("gen_ai_gateway_request_id", STRING),
            bigquery.SchemaField("gen_ai_gateway_response_id", STRING),
            bigquery.SchemaField("generation_id", STRING),
            bigquery.SchemaField("prev_step_id", STRING),
            bigquery.SchemaField("start_timestamp", TIMESTAMP),
            bigquery.SchemaField("end_timestamp", TIMESTAMP),
            bigquery.SchemaField("internal_organization_id", STRING),
            bigquery.SchemaField("data_source_id", STRING),
            bigquery.SchemaField("data_source_object_id", STRING),
            bigquery.SchemaField("external_source_id", STRING),
            bigquery.SchemaField("pre_step_variable_text", STRING),
            bigquery.SchemaField("post_step_variable_text", STRING),
            bigquery.SchemaField("attribute_text", STRING),
            bigquery.SchemaField("error_message_text", STRING),
            bigquery.SchemaField("sub_type", STRING),
            bigquery.SchemaField("output_value_text", STRING),
        ]
    ),
    "ai_agent_interaction_message_staging": add_base_fields(
        [
            bigquery.SchemaField("id", STRING),
            bigquery.SchemaField("ai_agent_interaction_id", STRING),
            bigquery.SchemaField("ai_agent_session_id", STRING),
            bigquery.SchemaField("ai_agent_session_participant_id", STRING),
            bigquery.SchemaField("ai_agent_interaction_message_type", STRING),
            bigquery.SchemaField("ai_agent_interaction_msg_content_type", STRING),
            bigquery.SchemaField("session_owner_id", STRING),
            bigquery.SchemaField("individual_id", STRING),
            bigquery.SchemaField("parent_message_id", STRING),
            bigquery.SchemaField("content_text", STRING),
            bigquery.SchemaField("message_sent_timestamp", TIMESTAMP),
            bigquery.SchemaField("message_start_timestamp", TIMESTAMP),
            bigquery.SchemaField("message_end_timestamp", TIMESTAMP),
            bigquery.SchemaField("internal_organization_id", STRING),
            bigquery.SchemaField("data_source_id", STRING),
            bigquery.SchemaField("data_source_object_id", STRING),
            bigquery.SchemaField("external_source_id", STRING),
            bigquery.SchemaField("modality", STRING),
        ]
    ),
    # --- Messaging ---
    "messaging_end_user": add_base_fields(
        [
            bigquery.SchemaField("id", STRING),
            bigquery.SchemaField("name", STRING),
            bigquery.SchemaField("messaging_channel_id", STRING),
            bigquery.SchemaField("message_type", STRING),
            bigquery.SchemaField("messaging_platform_key", STRING),
            bigquery.SchemaField("locale", STRING),
            bigquery.SchemaField("iso_country_code", STRING),
            bigquery.SchemaField("messaging_consent_status", STRING),
            bigquery.SchemaField("is_fully_opted_in", BOOL),
            bigquery.SchemaField("messaging_external_user_key", STRING),
            bigquery.SchemaField("created_date", TIMESTAMP),
            bigquery.SchemaField("last_modified_date", TIMESTAMP),
        ]
    ),
    "messaging_session": add_base_fields(
        [
            bigquery.SchemaField("id", STRING),
            bigquery.SchemaField("conversation_id", STRING),
            bigquery.SchemaField("status", STRING),
            bigquery.SchemaField("start_time", TIMESTAMP),
            bigquery.SchemaField("end_time", TIMESTAMP),
            bigquery.SchemaField("messaging_channel_id", STRING),
            bigquery.SchemaField("messaging_end_user_id", STRING),
            bigquery.SchemaField("origin", STRING),
            bigquery.SchemaField("created_date", TIMESTAMP),
            bigquery.SchemaField("last_modified_date", TIMESTAMP),
        ]
    ),
    # Staging das duas acima — write_mode='merge' (ver flow.py): carrega aqui via
    # load_chunk_to_staging, depois MERGE pra tabela final via merge_staging_to_target
    # (mesmo padrão de telemetry_trace_span_staging). Substituiu write_mode='replace',
    # que apagava a partição do dia inteiro a cada execução — seguro rodando 1x/dia,
    # mas destrutivo com o schedule de 15 em 15min (cada run apagava o que as runs
    # anteriores do mesmo dia já tinham carregado).
    "messaging_end_user_staging": add_base_fields(
        [
            bigquery.SchemaField("id", STRING),
            bigquery.SchemaField("name", STRING),
            bigquery.SchemaField("messaging_channel_id", STRING),
            bigquery.SchemaField("message_type", STRING),
            bigquery.SchemaField("messaging_platform_key", STRING),
            bigquery.SchemaField("locale", STRING),
            bigquery.SchemaField("iso_country_code", STRING),
            bigquery.SchemaField("messaging_consent_status", STRING),
            bigquery.SchemaField("is_fully_opted_in", BOOL),
            bigquery.SchemaField("messaging_external_user_key", STRING),
            bigquery.SchemaField("created_date", TIMESTAMP),
            bigquery.SchemaField("last_modified_date", TIMESTAMP),
        ]
    ),
    "messaging_session_staging": add_base_fields(
        [
            bigquery.SchemaField("id", STRING),
            bigquery.SchemaField("conversation_id", STRING),
            bigquery.SchemaField("status", STRING),
            bigquery.SchemaField("start_time", TIMESTAMP),
            bigquery.SchemaField("end_time", TIMESTAMP),
            bigquery.SchemaField("messaging_channel_id", STRING),
            bigquery.SchemaField("messaging_end_user_id", STRING),
            bigquery.SchemaField("origin", STRING),
            bigquery.SchemaField("created_date", TIMESTAMP),
            bigquery.SchemaField("last_modified_date", TIMESTAMP),
        ]
    ),
    "conversation_entry": add_base_fields(
        [
            bigquery.SchemaField("id", STRING),
            bigquery.SchemaField("conversation_id", STRING),
            bigquery.SchemaField("conversation_entry_type", STRING),
            bigquery.SchemaField("conversation_entry_visibility_type", STRING),
            bigquery.SchemaField("engagement_participant_id", STRING),
            bigquery.SchemaField("payload_text", STRING),
            bigquery.SchemaField("language", STRING),
            bigquery.SchemaField("duration_seconds_count", FLOAT64),
            bigquery.SchemaField("version_number", STRING),
            bigquery.SchemaField("external_record_id", STRING),
            bigquery.SchemaField("client_date_time", TIMESTAMP),
            bigquery.SchemaField("transcripted_date_time", TIMESTAMP),
            bigquery.SchemaField("created_date", TIMESTAMP),
            bigquery.SchemaField("last_modified_date", TIMESTAMP),
            bigquery.SchemaField("internal_organization_id", STRING),
            bigquery.SchemaField("data_source_id", STRING),
            bigquery.SchemaField("data_source_object_id", STRING),
            bigquery.SchemaField("kq_id", STRING),
        ]
    ),
    "conversation_entry_staging": add_base_fields(
        [
            bigquery.SchemaField("id", STRING),
            bigquery.SchemaField("conversation_id", STRING),
            bigquery.SchemaField("conversation_entry_type", STRING),
            bigquery.SchemaField("conversation_entry_visibility_type", STRING),
            bigquery.SchemaField("engagement_participant_id", STRING),
            bigquery.SchemaField("payload_text", STRING),
            bigquery.SchemaField("language", STRING),
            bigquery.SchemaField("duration_seconds_count", FLOAT64),
            bigquery.SchemaField("version_number", STRING),
            bigquery.SchemaField("external_record_id", STRING),
            bigquery.SchemaField("client_date_time", TIMESTAMP),
            bigquery.SchemaField("transcripted_date_time", TIMESTAMP),
            bigquery.SchemaField("created_date", TIMESTAMP),
            bigquery.SchemaField("last_modified_date", TIMESTAMP),
            bigquery.SchemaField("internal_organization_id", STRING),
            bigquery.SchemaField("data_source_id", STRING),
            bigquery.SchemaField("data_source_object_id", STRING),
            bigquery.SchemaField("kq_id", STRING),
        ]
    ),
    # --- Tracing ---
    "telemetry_trace_span": add_base_fields(
        [
            bigquery.SchemaField("id", STRING),
            bigquery.SchemaField("telemetry_trace", STRING),
            bigquery.SchemaField("telemetry_parent_span_id", STRING),
            bigquery.SchemaField("operation_name", STRING),
            bigquery.SchemaField("span_kind", STRING),
            bigquery.SchemaField("start_date_time", TIMESTAMP),
            bigquery.SchemaField("end_date_time", TIMESTAMP),
            bigquery.SchemaField("duration_number", FLOAT64),
            bigquery.SchemaField("status_code", STRING),
            bigquery.SchemaField("service_name", STRING),
            bigquery.SchemaField("telemetry_span_attribute_text", STRING),
            bigquery.SchemaField("data_source_id", STRING),
            bigquery.SchemaField("data_source_object_id", STRING),
            bigquery.SchemaField("internal_organization_id", STRING),
            bigquery.SchemaField("kq_id", STRING),
        ]
    ),
    "telemetry_trace_span_staging": add_base_fields(
        [
            bigquery.SchemaField("id", STRING),
            bigquery.SchemaField("telemetry_trace", STRING),
            bigquery.SchemaField("telemetry_parent_span_id", STRING),
            bigquery.SchemaField("operation_name", STRING),
            bigquery.SchemaField("span_kind", STRING),
            bigquery.SchemaField("start_date_time", TIMESTAMP),
            bigquery.SchemaField("end_date_time", TIMESTAMP),
            bigquery.SchemaField("duration_number", FLOAT64),
            bigquery.SchemaField("status_code", STRING),
            bigquery.SchemaField("service_name", STRING),
            bigquery.SchemaField("telemetry_span_attribute_text", STRING),
            bigquery.SchemaField("data_source_id", STRING),
            bigquery.SchemaField("data_source_object_id", STRING),
            bigquery.SchemaField("internal_organization_id", STRING),
            bigquery.SchemaField("kq_id", STRING),
        ]
    ),
    # --- Eventos WhatsApp ---
    "messaging_events_whatsapp": add_base_fields(list(MESSAGING_EVENTS_WHATSAPP_FIELDS)),
    "messaging_events_whatsapp_staging": add_base_fields(list(MESSAGING_EVENTS_WHATSAPP_FIELDS)),
    "sfmc_whatsapp_send_event": add_base_fields(list(SFMC_WHATSAPP_SEND_EVENT_FIELDS)),
    "sfmc_whatsapp_send_event_staging": add_base_fields(list(SFMC_WHATSAPP_SEND_EVENT_FIELDS)),
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
    "messaging_events_whatsapp": ["event_id"],
    "sfmc_whatsapp_send_event": ["id", "message_key"],
    # telemetry_trace_span_staging, messaging_end_user_staging, messaging_session_staging:
    # sem particionamento (staging tables)
}
