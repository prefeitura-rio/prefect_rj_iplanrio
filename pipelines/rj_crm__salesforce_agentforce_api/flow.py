# -*- coding: utf-8 -*-
"""
Flow orquestrador — Agentforce → BigQuery.

Executa as fases em sequência com pre-flight checks, tratamento de erros por
fase (não-críticos não abortam o pipeline) e notificação final no Slack.

Modo janela (desde 2026-09-08, ver tasks/janela.py e flows/template.py):
substitui o watermark forward-only (nunca revisita o passado, não se
recupera sozinho de um tick perdido) por duas passadas com sobreposição
intencional — `modo='hora'` roda de 15/15min com janela rolante de 1h,
`modo='dia'` roda 1x/dia reconciliando o dia inteiro. As duas escrevem via
staging + MERGE, nunca watermark. Ver prefect.yaml pros dois schedules e o
concurrency_limit que impede as duas passadas de escreverem na mesma tabela
ao mesmo tempo.

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
    get_data_cloud_session,
)
from pipelines.rj_crm__salesforce_agentforce_api.tasks.janela import janela_dia, janela_hora
from pipelines.rj_crm__salesforce_agentforce_api.tasks.notify import (
    notify_pipeline_summary,
    notify_phase_failure,
)
from pipelines.rj_crm__salesforce_agentforce_api.tasks.ensure_tables import ensure_bq_tables
from pipelines.rj_crm__salesforce_agentforce_api.tasks.preflight import run_preflight_checks


# ---------------------------------------------------------------------------
# Queries inline (podem ser externalizadas para settings.yaml futuramente)
# Todas com {data_inicio}/{data_fim} — modo janela, ver docstring do módulo.
# ---------------------------------------------------------------------------

_F2A_QUERIES = {
    "conversation_entry": """
        SELECT
            ssot__Id__c,
            ssot__ConversationId__c,
            ssot__ConversationEntryType__c,
            ssot__ConversationEntryVisibilityType__c,
            ssot__EngagementParticipantId__c,
            ssot__PayloadText__c,
            ssot__Language__c,
            ssot__DurationSecondsCount__c,
            ssot__VersionNumber__c,
            ssot__ExternalRecordId__c,
            ssot__ClientDateTime__c,
            ssot__TranscriptedDateTime__c,
            ssot__CreatedDate__c,
            ssot__LastModifiedDate__c,
            ssot__InternalOrganizationId__c,
            ssot__DataSourceId__c,
            ssot__DataSourceObjectId__c,
            KQ_Id__c
        FROM ssot__ConversationEntry__dlm
        WHERE ssot__CreatedDate__c >= '{data_inicio}'
          AND ssot__CreatedDate__c <  '{data_fim}'
    """,
}

_F2A_CRM_QUERIES = {
    "messaging_end_user": {
        "soql": "SELECT Id, Name, MessagingChannelId, MessageType, MessagingPlatformKey, Locale, IsoCountryCode, MessagingConsentStatus, IsFullyOptedIn, MessagingExternalUserKey, CreatedDate, LastModifiedDate FROM MessagingEndUser WHERE LastModifiedDate >= {data_inicio} AND LastModifiedDate < {data_fim} ORDER BY CreatedDate ASC",
        "date_columns": ["created_date", "last_modified_date"],
        "clustering_fields": ["id"],
        "staging_table": "messaging_end_user_staging",
    },
    "messaging_session": {
        "soql": "SELECT Id, Status, StartTime, EndTime, MessagingChannelId, MessagingEndUserId, Origin, CreatedDate, LastModifiedDate FROM MessagingSession WHERE LastModifiedDate >= {data_inicio} AND LastModifiedDate < {data_fim} ORDER BY CreatedDate ASC",
        "date_columns": ["start_time", "end_time", "created_date", "last_modified_date"],
        "clustering_fields": ["id"],
        "staging_table": "messaging_session_staging",
    },
}

_F3_QUERY = """
    SELECT
        ssot__Id__c,
        ssot__TelemetryTrace__c,
        ssot__TelemetryParentSpanId__c,
        ssot__OperationName__c,
        ssot__SpanKind__c,
        ssot__StartDateTime__c,
        ssot__EndDateTime__c,
        ssot__DurationNumber__c,
        ssot__StatusCode__c,
        ssot__ServiceName__c,
        ssot__TelemetrySpanAttributeText__c,
        ssot__DataSourceId__c,
        ssot__DataSourceObjectId__c,
        ssot__InternalOrganizationId__c,
        KQ_Id__c
    FROM ssot__TelemetryTraceSpan__dlm
    WHERE ssot__StartDateTime__c >= '{data_inicio}'
      AND ssot__StartDateTime__c <  '{data_fim}'
"""


# ---------------------------------------------------------------------------
# Flow principal
# ---------------------------------------------------------------------------


@flow(name="agentforce-full-daily", log_prints=True)
def agentforce_full_daily(
    project_id: str | None = None,
    dataset_id: str | None = None,
    control_dataset: str | None = None,
    partition_date: date | None = None,
    modo: str = "hora",
    run_phases: list[int] | None = None,
    environment: str = "prod",
) -> dict[str, dict[str, int]]:
    """
    Orquestra as fases da pipeline Agentforce → BigQuery.

    Args:
        project_id      : ID do projeto GCP.
        dataset_id      : Dataset BQ de destino.
        control_dataset : Dataset de controle. Não usado em modo janela (F1/F2a/F3
                          não tocam mais no checkpoint) — mantido por compatibilidade.
        partition_date  : Só relevante em modo='dia' — qual dia reconciliar.
                          Padrão: ontem.
        modo            : 'hora' (padrão, janela rolante de 1h — schedule de
                          15/15min) ou 'dia' (dia inteiro — schedule 1x/dia,
                          reconciliação funda). Ver tasks/janela.py.
        run_phases      : Lista de fases a executar. Padrão: [1, 2, 3, 4].
                          Use [1] para rodar apenas F1 em testes.
        environment     : Ambiente de execução ('prod' ou 'staging').

    Returns:
        Dict por fase com {tabela: linhas_afetadas}.
    """
    project_id = project_id or AgentforceConstants.BQ_PROJECT_ID.value
    dataset_id = dataset_id or AgentforceConstants.DATASET_ID.value
    control_dataset = control_dataset or AgentforceConstants.CONTROL_DATASET.value
    run_phases = run_phases or [1, 2, 3, 4]

    rename_current_flow_run_task(new_name="agentforce-full-daily")
    inject_bd_credentials_task(environment=environment)

    ensure_bq_tables(project_id=project_id, dataset_id=dataset_id)

    t_pipeline_start = time.time()
    phase_results: dict[str, dict[str, int]] = {}

    if modo == "dia":
        from datetime import timedelta
        dia_alvo = partition_date or (date.today() - timedelta(days=1))
        data_inicio, data_fim, partition_date_efetivo = janela_dia(dia_alvo)
    else:
        data_inicio, data_fim, partition_date_efetivo = janela_hora()

    print(f"[DAILY] modo='{modo}' janela=[{data_inicio}, {data_fim}) partition_date={partition_date_efetivo}")

    bq_base = dict(
        project_id=project_id,
        dataset_id=dataset_id,
        control_dataset=control_dataset,
        partition_date=partition_date_efetivo,
        write_mode="merge",
        janela=(data_inicio, data_fim),
    )

    # Auth
    dc_session = get_data_cloud_session()

    # Pre-flight
    preflight = run_preflight_checks(
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
                modo=modo,
                environment=environment,
            )
            phase_results["F1 - STDM"] = f1_rows
            print(f"[DAILY] F1 concluida em {time.time() - t0:.0f}s")
        except Exception as exc:
            # F1 é crítica — re-raise
            notify_phase_failure(phase_name="F1 — STDM", error_message=str(exc))
            raise

    # -------------------------------------------------------------------------
    # Fase 2a — Messaging CRM (não-crítica)
    # messaging_end_user e messaging_session via CRM REST API
    # conversation_entry via Data Cloud
    # -------------------------------------------------------------------------
    if 2 in run_phases:
        t0 = time.time()
        f2a_rows: dict[str, int] = {}
        try:
            # CRM REST usa as mesmas credenciais do Data Cloud (client_credentials).
            # write_mode='merge' sempre — staging + MERGE por Id, obrigatório em
            # modo janela (a sobreposição entre janelas recaptura o mesmo id de
            # propósito; sem merge duplicaria a cada tick).
            for table, cfg in _F2A_CRM_QUERIES.items():
                f2a_rows[table] = sf_to_bq(
                    source="crm_rest",
                    query_template=cfg["soql"],
                    target_table=table,
                    crm_session=dc_session,
                    date_columns=cfg["date_columns"],
                    clustering_fields=cfg["clustering_fields"],
                    staging_table=cfg["staging_table"],
                    primary_key="id",
                    **bq_base,
                )

            if preflight.get("dc_auth"):
                f2a_rows["conversation_entry"] = sf_to_bq(
                    source="data_cloud",
                    query_template=_F2A_QUERIES["conversation_entry"],
                    target_table="conversation_entry",
                    dc_session=dc_session,
                    is_data_cloud=True,
                    date_columns=["client_date_time", "transcripted_date_time", "created_date", "last_modified_date"],
                    clustering_fields=["id"],
                    staging_table="conversation_entry_staging",
                    primary_key="id",
                    **bq_base,
                )
            phase_results["F2a - Messaging"] = f2a_rows
            print(f"[DAILY] F2a concluida em {time.time() - t0:.0f}s")
        except Exception as exc:
            print(f"[DAILY] WARN: F2a falhou — {exc}. Continuando...")
            notify_phase_failure(phase_name="F2a — Messaging", error_message=str(exc))
            phase_results["F2a - Messaging"] = f2a_rows

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
                date_columns=["start_date_time", "end_date_time"],
                primary_key="id",
                clustering_fields=["id"],
                **bq_base,
            )
            phase_results["F3 - Tracing"] = {"telemetry_trace_span": rows}
            print(f"[DAILY] F3 concluida em {time.time() - t0:.0f}s")
        except Exception as exc:
            print(f"[DAILY] WARN: F3 falhou — {exc}.")
            notify_phase_failure(phase_name="F3 — Platform Tracing", error_message=str(exc))
            phase_results["F3 - Tracing"] = {"telemetry_trace_span": 0}
    elif 3 in run_phases:
        print("[DAILY] F3 (Platform Tracing) pulada — DMO nao disponível.")

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
