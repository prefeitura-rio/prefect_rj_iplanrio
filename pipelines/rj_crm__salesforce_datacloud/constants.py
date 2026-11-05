"""Constants of rj_crm__salesforce_datacloud."""

from typing import Final
from zoneinfo import ZoneInfo

from google.cloud import bigquery

from pipelines.rj_crm__salesforce_datacloud.types import Feed

DEFAULT_PROJECT_ID: Final = "rj-crm-registry"
DEFAULT_DATASET_ID: Final = "brutos_salesforce"
SP_TZ: Final = ZoneInfo("America/Sao_Paulo")
WINDOW_FORMAT: Final = "%Y-%m-%dT%H:%M:%SZ"
"""The format of the windows.

The `Z` is the way the source writes its time, not a zone.
"""
QUERY_PATH: Final = "/services/data/v67.0/ssot/query-sql"
CRM_QUERY_PATH: Final = "/services/data/v67.0/query"
WORKLOAD: Final = "BatchQuery"
PAGE_SIZE: Final = 1000
"""Rows of a Data Cloud page, under the 1400 to 1700 that one response held."""
MAX_ROWS: Final = 5_000_000
"""A stop for a pagination that does not end."""
TIMEOUT: Final = 120
CRM_TIMEOUT: Final = 60
LOGIN_TIMEOUT: Final = 30
RETRY_DELAYS: Final = (30, 60, 120)
RETRIES: Final = 3
DISCORD_NAME: Final = "salesforce-datacloud"
DISCORD_LIMIT: Final = 2000
STEP_TYPE: Final = "ACTION_STEP"
PARTITION_COLUMN: Final = "data_particao"
FEEDS: Final = (
    Feed(
        "ai_agent_session",
        "Agentforce",
        "ai_agent_session",
        date_columns=("start_timestamp", "end_timestamp"),
        critical=True,
    ),
    Feed(
        "ai_agent_interaction",
        "Agentforce",
        "ai_agent_interaction",
        date_columns=("start_timestamp", "end_timestamp"),
        critical=True,
    ),
    Feed(
        "ai_agent_interaction_step",
        "Agentforce",
        "ai_agent_interaction_step",
        date_columns=("start_timestamp", "end_timestamp"),
        critical=True,
        step_output_only=True,
    ),
    Feed(
        "ai_agent_interaction_message",
        "Agentforce",
        "ai_agent_interaction_message",
        date_columns=("message_sent_timestamp", "message_start_timestamp", "message_end_timestamp"),
        critical=True,
    ),
    Feed(
        "messaging_end_user",
        "Messaging",
        "messaging_end_user",
        source="crm_rest",
        date_columns=("created_date", "last_modified_date"),
    ),
    Feed(
        "messaging_session",
        "Messaging",
        "messaging_session",
        source="crm_rest",
        date_columns=("start_time", "end_time", "created_date", "last_modified_date"),
    ),
    Feed(
        "conversation_entry",
        "Messaging",
        "conversation_entry",
        date_columns=("client_date_time", "transcripted_date_time", "created_date", "last_modified_date"),
    ),
    Feed("telemetry_trace_span", "Tracing", "telemetry_trace_span", date_columns=("start_date_time", "end_date_time")),
    Feed(
        "messaging_events_whatsapp",
        "Eventos WhatsApp",
        "messaging_events_whatsapp",
        date_columns=("event_date_time", "cdp_sys_partition_date"),
        primary_key="event_id",
        order_by="EventId__c",
        utc_window=True,
    ),
    Feed(
        "sfmc_whatsapp_send_event",
        "Eventos WhatsApp",
        "sfmc_whatsapp_send_event",
        date_columns=("engagement_date_time", "accepted_time_utc", "last_modified_date", "cdp_sys_partition_date"),
        order_by="ID__c",
        utc_window=True,
    ),
)
"""The tables to extract, in the order that they run.

The Agentforce ones go session, interaction, step, message.
"""


TIMESTAMP = bigquery.enums.SqlTypeNames.TIMESTAMP


STRING = bigquery.enums.SqlTypeNames.STRING


FLOAT64 = bigquery.enums.SqlTypeNames.FLOAT64


BOOL = bigquery.enums.SqlTypeNames.BOOLEAN


# Eventos WhatsApp (DLL MessagingEventsWhatsAppV2_00Das_4CAB1BC2__dll). Chave do MERGE:
# event_id (EventId__c), conferido único e nunca vazio no histórico todo em 2026-09-29.
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


# Send events WhatsApp do SFMC (DLL sfmc_whatsapp_send_event_534019838__dll). Chave do
# MERGE: id (ID__c), conferido único e nunca vazio no histórico todo em 2026-10-02.
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
    # telemetry_trace_span_staging, messaging_end_user_staging,
    # messaging_session_staging: sem particionamento (staging tables)
}
