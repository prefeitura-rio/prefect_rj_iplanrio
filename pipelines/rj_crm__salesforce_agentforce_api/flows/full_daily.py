# -*- coding: utf-8 -*-
"""
Flow orquestrador diário — Agentforce → BigQuery.

Executa as 4 fases em sequência com pre-flight checks, tratamento de erros
por fase (não-críticos não abortam o pipeline) e notificação final no Slack.

Timeline típica (03:00 UTC):
  03:00  Pre-flight checks (30s)
  03:01  Fase 1 — STDM (~2min)
  03:03  Fase 2a — Messaging (~1min)
  03:04  Fase 2b — MCE (~2min, opcional)
  03:06  Fase 3 — Platform Tracing (~3min, opcional, chunked)
  03:09  Fase 4 — GenAI Audit (~1min, opcional)
  03:10  Notificação Slack

Fases opcionais (2b, 3, 4): se a DMO não existir no Data Cloud,
o pre-flight retorna False e a fase é pulada com warning.
Fase 1 é obrigatória — falha aborta tudo.
"""

from __future__ import annotations

import time
from datetime import date

from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_crm__salesforce_agentforce_api.constants import AgentforceConstants
from pipelines.rj_crm__salesforce_agentforce_api.flows.template import sf_to_bq
from pipelines.rj_crm__salesforce_agentforce_api.tasks.auth import (
    get_bulk_api_session,
    get_data_cloud_session,
)
from pipelines.rj_crm__salesforce_agentforce_api.tasks.notify import (
    notify_pipeline_summary,
    notify_phase_failure,
)
from pipelines.rj_crm__salesforce_agentforce_api.tasks.preflight import run_preflight_checks


# ---------------------------------------------------------------------------
# Queries inline (podem ser externalizadas para settings.yaml futuramente)
# ---------------------------------------------------------------------------

_F2A_QUERIES = {
    "messaging_session": """
        SELECT Id, Status, StartTime, EndTime, MessagingChannel, Origin,
               CreatedDate, LastModifiedDate
        FROM MessagingSession
        WHERE LastModifiedDate >= {watermark}
    """,
    "conversation_entry": """
        SELECT ssot__Id__c, ssot__ConversationId__c, ssot__ActorType__c,
               ssot__EntryType__c, ssot__Message__c, ssot__EntryPayload__c,
               ssot__CreatedDate__c
        FROM ssot__ConversationEntry__dlm
        WHERE ssot__CreatedDate__c >= '{watermark}'
    """,
}

_F2B_MCE_QUERIES = {
    "mce_sent": "SELECT ssot__Id__c, ssot__SubscriberId__c, ssot__JobId__c, ssot__EventDate__c, ssot__CreatedDate__c FROM ssot__MCE_Sent__dlm WHERE ssot__EventDate__c >= '{watermark}'",
    "mce_open": "SELECT ssot__Id__c, ssot__SubscriberId__c, ssot__JobId__c, ssot__EventDate__c, ssot__CreatedDate__c FROM ssot__MCE_Open__dlm WHERE ssot__EventDate__c >= '{watermark}'",
    "mce_click": "SELECT ssot__Id__c, ssot__SubscriberId__c, ssot__JobId__c, ssot__LinkUrl__c, ssot__EventDate__c, ssot__CreatedDate__c FROM ssot__MCE_Click__dlm WHERE ssot__EventDate__c >= '{watermark}'",
    "mce_bounce": "SELECT ssot__Id__c, ssot__SubscriberId__c, ssot__JobId__c, ssot__BounceCategory__c, ssot__EventDate__c, ssot__CreatedDate__c FROM ssot__MCE_Bounce__dlm WHERE ssot__EventDate__c >= '{watermark}'",
    "mce_unsub": "SELECT ssot__Id__c, ssot__SubscriberId__c, ssot__JobId__c, ssot__EventDate__c, ssot__CreatedDate__c FROM ssot__MCE_Unsub__dlm WHERE ssot__EventDate__c >= '{watermark}'",
    "mce_subscriber": "SELECT ssot__Id__c, ssot__SubscriberKey__c, ssot__EmailAddress__c, ssot__Status__c, ssot__CreatedDate__c FROM ssot__MCE_Subscriber__dlm WHERE ssot__CreatedDate__c >= '{watermark}'",
}

_F3_QUERY = """
    SELECT
        ssot__Id__c, ssot__TraceId__c, ssot__SpanId__c, ssot__ParentSpanId__c,
        ssot__SpanName__c, ssot__SpanKind__c, ssot__StartTimeNs__c,
        ssot__EndTimeNs__c, ssot__DurationNs__c, ssot__Status__c,
        ssot__ServiceName__c, ssot__CreatedDate__c
    FROM ssot__TelemetryTraceSpan__dlm
    WHERE ssot__CreatedDate__c >= '{watermark}'
"""

_F4_QUERIES = {
    "genai_gateway_request": "SELECT ssot__Id__c, ssot__Model__c, ssot__RequestTokens__c, ssot__ResponseTokens__c, ssot__LatencyMs__c, ssot__Status__c, ssot__CreatedDate__c FROM ssot__GenAIGatewayRequest__dlm WHERE ssot__CreatedDate__c >= '{watermark}'",
    "genai_generation": "SELECT ssot__Id__c, ssot__RequestId__c, ssot__GeneratedText__c, ssot__FinishReason__c, ssot__CreatedDate__c FROM ssot__GenAIGeneration__dlm WHERE ssot__CreatedDate__c >= '{watermark}'",
    "genai_quality": "SELECT ssot__Id__c, ssot__GenerationId__c, ssot__QualityScore__c, ssot__QualityType__c, ssot__CreatedDate__c FROM ssot__GenAIQuality__dlm WHERE ssot__CreatedDate__c >= '{watermark}'",
    "genai_feedback": "SELECT ssot__Id__c, ssot__GenerationId__c, ssot__FeedbackType__c, ssot__Rating__c, ssot__Comment__c, ssot__CreatedDate__c FROM ssot__GenAIFeedback__dlm WHERE ssot__CreatedDate__c >= '{watermark}'",
}


# ---------------------------------------------------------------------------
# Flow principal
# ---------------------------------------------------------------------------


@flow(name="agentforce-full-daily", log_prints=True)
def agentforce_full_daily(
    project_id: str | None = None,
    dataset_id: str | None = None,
    control_dataset: str | None = None,
    partition_date: date | None = None,
    skip_checkpoint: bool = False,
    run_phases: list[int] | None = None,
) -> dict[str, dict[str, int]]:
    """
    Orquestra as 4 fases da pipeline Agentforce → BigQuery.

    Args:
        project_id      : ID do projeto GCP.
        dataset_id      : Dataset BQ de destino.
        control_dataset : Dataset de controle (watermarks).
        partition_date  : Data de partição. Padrão: hoje.
        skip_checkpoint : Se True, não usa watermark (backfill).
        run_phases      : Lista de fases a executar. Padrão: [1, 2, 3, 4].
                          Use [1] para rodar apenas F1 em testes.

    Returns:
        Dict por fase com {tabela: linhas_carregadas}.
    """
    project_id = project_id or AgentforceConstants.BQ_PROJECT_ID.value
    dataset_id = dataset_id or AgentforceConstants.DATASET_ID.value
    control_dataset = control_dataset or AgentforceConstants.CONTROL_DATASET.value
    run_phases = run_phases or [1, 2, 3, 4]

    rename_current_flow_run_task(new_name="agentforce-full-daily")
    inject_bd_credentials_task(environment="prod")

    t_pipeline_start = time.time()
    phase_results: dict[str, dict[str, int]] = {}

    bq_base = dict(
        project_id=project_id,
        dataset_id=dataset_id,
        control_dataset=control_dataset,
        partition_date=partition_date,
        skip_checkpoint=skip_checkpoint,
    )

    # Auth
    bulk_session = get_bulk_api_session()
    dc_session = get_data_cloud_session()

    # Pre-flight
    preflight = run_preflight_checks(
        bulk_session=bulk_session,
        dc_session=dc_session,
        bq_project_id=project_id,
        bq_dataset_id=dataset_id,
    )

    # -------------------------------------------------------------------------
    # Fase 1 — STDM (obrigatória)
    # -------------------------------------------------------------------------
    if 1 in run_phases:
        from pipelines.rj_crm__salesforce_agentforce_api.flows.fase1_stdm import fase1_stdm
        t0 = time.time()
        try:
            f1_rows = fase1_stdm(
                project_id=project_id,
                dataset_id=dataset_id,
                control_dataset=control_dataset,
                partition_date=partition_date,
                skip_checkpoint=skip_checkpoint,
            )
            phase_results["F1 - STDM"] = f1_rows
            print(f"[DAILY] F1 concluida em {time.time() - t0:.0f}s")
        except Exception as exc:
            # F1 é crítica — re-raise
            notify_phase_failure(phase_name="F1 — STDM", error_message=str(exc))
            raise

    # -------------------------------------------------------------------------
    # Fase 2a — Messaging (não-crítica)
    # -------------------------------------------------------------------------
    if 2 in run_phases:
        t0 = time.time()
        f2a_rows: dict[str, int] = {}
        try:
            f2a_rows["messaging_session"] = sf_to_bq(
                source="bulk_api",
                query_template=_F2A_QUERIES["messaging_session"],
                target_table="messaging_session",
                bulk_session=bulk_session,
                **bq_base,
            )
            if preflight.get("dc_auth"):
                f2a_rows["conversation_entry"] = sf_to_bq(
                    source="data_cloud",
                    query_template=_F2A_QUERIES["conversation_entry"],
                    target_table="conversation_entry",
                    dc_session=dc_session,
                    is_data_cloud=True,
                    **bq_base,
                )
            phase_results["F2a - Messaging"] = f2a_rows
            print(f"[DAILY] F2a concluida em {time.time() - t0:.0f}s")
        except Exception as exc:
            print(f"[DAILY] WARN: F2a falhou — {exc}. Continuando...")
            notify_phase_failure(phase_name="F2a — Messaging", error_message=str(exc))
            phase_results["F2a - Messaging"] = f2a_rows

        # Fase 2b — MCE (opcional)
        if preflight.get("dmos_mce"):
            f2b_rows: dict[str, int] = {}
            try:
                for table, query in _F2B_MCE_QUERIES.items():
                    f2b_rows[table] = sf_to_bq(
                        source="data_cloud",
                        query_template=query,
                        target_table=table,
                        dc_session=dc_session,
                        is_data_cloud=True,
                        **bq_base,
                    )
                phase_results["F2b - MCE"] = f2b_rows
                print(f"[DAILY] F2b concluida.")
            except Exception as exc:
                print(f"[DAILY] WARN: F2b falhou — {exc}. Continuando...")
                notify_phase_failure(phase_name="F2b — MCE", error_message=str(exc))
                phase_results["F2b - MCE"] = f2b_rows
        else:
            print("[DAILY] F2b (MCE) pulada — DMOs nao disponíveis.")

    # -------------------------------------------------------------------------
    # Fase 3 — Platform Tracing (opcional, chunked)
    # -------------------------------------------------------------------------
    if 3 in run_phases and preflight.get("dmos_tracing"):
        t0 = time.time()
        try:
            rows = sf_to_bq(
                source="data_cloud_chunked",
                query_template=_F3_QUERY,
                target_table="telemetry_trace_span",
                staging_table="telemetry_trace_span_staging",
                dc_session=dc_session,
                is_data_cloud=True,
                duration_ns_columns=["duration_ns", "start_time_ns", "end_time_ns"],
                write_mode="merge",
                primary_key="id",
                **bq_base,
            )
            phase_results["F3 - Tracing"] = {"telemetry_trace_span": rows}
            print(f"[DAILY] F3 concluida em {time.time() - t0:.0f}s")
        except Exception as exc:
            print(f"[DAILY] WARN: F3 falhou — {exc}. Checkpoint NAO atualizado.")
            notify_phase_failure(phase_name="F3 — Platform Tracing", error_message=str(exc))
            phase_results["F3 - Tracing"] = {"telemetry_trace_span": 0}
    elif 3 in run_phases:
        print("[DAILY] F3 (Platform Tracing) pulada — DMO nao disponível.")

    # -------------------------------------------------------------------------
    # Fase 4 — GenAI Audit (opcional)
    # -------------------------------------------------------------------------
    if 4 in run_phases and preflight.get("dmos_genai"):
        t0 = time.time()
        f4_rows: dict[str, int] = {}
        try:
            for table, query in _F4_QUERIES.items():
                f4_rows[table] = sf_to_bq(
                    source="data_cloud",
                    query_template=query,
                    target_table=table,
                    dc_session=dc_session,
                    is_data_cloud=True,
                    **bq_base,
                )
            phase_results["F4 - GenAI"] = f4_rows
            print(f"[DAILY] F4 concluida em {time.time() - t0:.0f}s")
        except Exception as exc:
            print(f"[DAILY] WARN: F4 falhou — {exc}. Continuando...")
            notify_phase_failure(phase_name="F4 — GenAI Audit", error_message=str(exc))
            phase_results["F4 - GenAI"] = f4_rows
    elif 4 in run_phases:
        print("[DAILY] F4 (GenAI Audit) pulada — DMOs nao disponíveis.")

    # -------------------------------------------------------------------------
    # Notificação final
    # -------------------------------------------------------------------------
    total_elapsed = time.time() - t_pipeline_start
    notify_pipeline_summary(
        phase_results=phase_results,
        total_elapsed_seconds=total_elapsed,
    )

    print(f"[DAILY] Pipeline concluido em {total_elapsed:.0f}s | {phase_results}")
    return phase_results
