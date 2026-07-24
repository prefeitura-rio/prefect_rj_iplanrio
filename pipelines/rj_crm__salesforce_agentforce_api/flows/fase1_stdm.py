# -*- coding: utf-8 -*-
"""
Fase 1 — STDM (Salesforce Trusted Data Model / Agentforce Data Model).

Extrai as 5 DMOs principais do Agentforce via Data Cloud Query API:
  - AiAgentSession
  - AiAgentInteraction
  - AiAgentInteractionStep
  - AiAgentInteractionMessage
  - AiAgentInteractionParticipant

DAG explícito:
  session → interaction → (steps | messages | participant em paralelo)

A ordem é respeitada para garantir integridade referencial no BQ.
"""

from __future__ import annotations

import time
from datetime import date

from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from prefect import flow

from pipelines.rj_crm__salesforce_agentforce_api.constants import AgentforceConstants
from pipelines.rj_crm__salesforce_agentforce_api.flows.template import sf_to_bq
from pipelines.rj_crm__salesforce_agentforce_api.tasks.auth import (
    get_bulk_api_session,
    get_data_cloud_session,
)
from pipelines.rj_crm__salesforce_agentforce_api.tasks.notify import (
    notify_phase_failure,
    notify_phase_success,
)


# Queries F1 — colunas explícitas, filtro de watermark via {watermark}
# Nomes de tabela: ssot__<Nome>__dlm  (prefixo ssot__ obrigatório)
# Colunas verificadas via SELECT * LIMIT 1 em 2026-07-23
_QUERIES = {
    "ai_agent_session": """
        SELECT
            ssot__Id__c,
            ssot__RelatedMessagingSessionId__c,
            ssot__AiAgentChannelType__c,
            ssot__AiAgentSessionEndType__c,
            ssot__SessionOwnerId__c,
            ssot__SessionOwnerObject__c,
            ssot__IndividualId__c,
            ssot__PreviousSessionId__c,
            ssot__StartTimestamp__c,
            ssot__EndTimestamp__c,
            ssot__InternalOrganizationId__c,
            ssot__DataSourceId__c,
            ssot__DataSourceObjectId__c,
            ssot__ExternalSourceId__c,
            ssot__VariableText__c
        FROM ssot__AiAgentSession__dlm
        WHERE ssot__StartTimestamp__c >= '{watermark}'
    """,
    "ai_agent_interaction": """
        SELECT
            ssot__Id__c,
            ssot__AiAgentSessionId__c,
            ssot__AiAgentInteractionType__c,
            ssot__TopicApiName__c,
            ssot__SessionOwnerId__c,
            ssot__SessionOwnerObject__c,
            ssot__IndividualId__c,
            ssot__TelemetryTraceId__c,
            ssot__TelemetryTraceSpanId__c,
            ssot__PrevInteractionId__c,
            ssot__StartTimestamp__c,
            ssot__EndTimestamp__c,
            ssot__InternalOrganizationId__c,
            ssot__DataSourceId__c,
            ssot__DataSourceObjectId__c,
            ssot__ExternalSourceId__c,
            ssot__AttributeText__c
        FROM ssot__AiAgentInteraction__dlm
        WHERE ssot__StartTimestamp__c >= '{watermark}'
    """,
    "ai_agent_interaction_step": """
        SELECT
            ssot__Id__c,
            ssot__AiAgentInteractionId__c,
            ssot__AiAgentInteractionStepType__c,
            ssot__Name__c,
            ssot__TelemetryTraceSpanId__c,
            ssot__GenAiGatewayRequestId__c,
            ssot__GenAiGatewayResponseId__c,
            ssot__GenerationId__c,
            ssot__PrevStepId__c,
            ssot__StartTimestamp__c,
            ssot__EndTimestamp__c,
            ssot__InternalOrganizationId__c,
            ssot__DataSourceId__c,
            ssot__DataSourceObjectId__c,
            ssot__ExternalSourceId__c,
            ssot__InputValueText__c,
            ssot__OutputValueText__c,
            ssot__PreStepVariableText__c,
            ssot__PostStepVariableText__c,
            ssot__AttributeText__c,
            ssot__ErrorMessageText__c,
            SubType__c
        FROM ssot__AiAgentInteractionStep__dlm
        WHERE ssot__StartTimestamp__c >= '{watermark}'
    """,
    "ai_agent_interaction_message": """
        SELECT
            ssot__Id__c,
            ssot__AiAgentInteractionId__c,
            ssot__AiAgentSessionId__c,
            ssot__AiAgentSessionParticipantId__c,
            ssot__AiAgentInteractionMessageType__c,
            ssot__AiAgentInteractionMsgContentType__c,
            ssot__SessionOwnerId__c,
            ssot__IndividualId__c,
            ssot__ParentMessageId__c,
            ssot__ContentText__c,
            ssot__MessageSentTimestamp__c,
            MessageStartTimestamp__c,
            MessageEndTimestamp__c,
            ssot__InternalOrganizationId__c,
            ssot__DataSourceId__c,
            ssot__DataSourceObjectId__c,
            ssot__ExternalSourceId__c,
            Modality__c
        FROM ssot__AiAgentInteractionMessage__dlm
        WHERE ssot__MessageSentTimestamp__c >= '{watermark}'
    """,
    # AiAgentInteractionParticipant__dlm não existe neste org (verificado 2026-07-23)
}

_DATE_COLS = ["start_timestamp", "end_timestamp", "message_sent_timestamp",
              "message_start_timestamp", "message_end_timestamp"]


@flow(name="agentforce-fase1-stdm", log_prints=True)
def fase1_stdm(
    project_id: str | None = None,
    dataset_id: str | None = None,
    control_dataset: str | None = None,
    partition_date: date | None = None,
    skip_checkpoint: bool = False,
) -> dict[str, int]:
    """
    Flow da Fase 1 — extrai as 5 DMOs STDM do Agentforce.

    Pode ser chamado standalone ou pelo flow orquestrador (full_daily.py).

    Args:
        project_id      : ID do projeto GCP. Padrão: constante.
        dataset_id      : Dataset BQ de destino. Padrão: constante.
        control_dataset : Dataset de controle (watermarks). Padrão: constante.
        partition_date  : Data de partição. Padrão: hoje.
        skip_checkpoint : Se True, não usa watermark (útil para backfill).

    Returns:
        Dict {tabela: linhas_carregadas}.
    """
    project_id = project_id or AgentforceConstants.BQ_PROJECT_ID.value
    dataset_id = dataset_id or AgentforceConstants.DATASET_ID.value
    control_dataset = control_dataset or AgentforceConstants.CONTROL_DATASET.value

    inject_bd_credentials_task(environment="prod")

    t_start = time.time()
    rows_by_table: dict[str, int] = {}

    try:
        # Auth — Data Cloud usa Client Credentials (sem username/password)
        bulk_session = get_bulk_api_session()
        dc_session = get_data_cloud_session()

        bq_args = dict(
            project_id=project_id,
            dataset_id=dataset_id,
            control_dataset=control_dataset,
            dc_session=dc_session,
            source="data_cloud",
            is_data_cloud=True,
            date_columns=_DATE_COLS,
            write_mode="append",
            partition_date=partition_date,
            skip_checkpoint=skip_checkpoint,
        )

        # DAG: session → interaction → (steps | messages)
        # Sequencial para garantir integridade referencial
        # Nota: AiAgentInteractionParticipant__dlm não existe neste org.

        rows_by_table["ai_agent_session"] = sf_to_bq(
            query_template=_QUERIES["ai_agent_session"],
            target_table="ai_agent_session",
            **bq_args,
        )

        rows_by_table["ai_agent_interaction"] = sf_to_bq(
            query_template=_QUERIES["ai_agent_interaction"],
            target_table="ai_agent_interaction",
            **bq_args,
        )

        # Steps e messages podem rodar em paralelo (sem dependência entre si)
        # Nota: Prefect tasks concorrentes exigem .submit() — aqui chamamos sequencialmente
        # por simplicidade. Para paralelismo real, use task_runner=ConcurrentTaskRunner.
        rows_by_table["ai_agent_interaction_step"] = sf_to_bq(
            query_template=_QUERIES["ai_agent_interaction_step"],
            target_table="ai_agent_interaction_step",
            **bq_args,
        )

        rows_by_table["ai_agent_interaction_message"] = sf_to_bq(
            query_template=_QUERIES["ai_agent_interaction_message"],
            target_table="ai_agent_interaction_message",
            **bq_args,
        )

        elapsed = time.time() - t_start
        notify_phase_success(
            phase_name="F1 — STDM",
            rows_by_table=rows_by_table,
            elapsed_seconds=elapsed,
        )

        print(f"[F1] Fase 1 concluida em {elapsed:.0f}s | {rows_by_table}")
        return rows_by_table

    except Exception as exc:
        notify_phase_failure(
            phase_name="F1 — STDM",
            error_message=str(exc),
        )
        raise
