"""Constants of rj_crm__salesforce_datacloud."""

from typing import Final
from zoneinfo import ZoneInfo

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
