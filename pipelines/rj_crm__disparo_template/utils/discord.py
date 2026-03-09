# -*- coding: utf-8 -*-
# flake8: noqa:E501
# pylint: disable='line-too-long'

"""
Módulo de notificações Discord para pipeline de disparo PIC lembrete.
"""

import asyncio
import json
import os
from datetime import datetime

import aiohttp  # pylint: disable=E0611, E0401
from discord import Webhook  # pylint: disable=E0611, E0401
from pytz import timezone
from prefect.client.schemas.objects import Flow, FlowRun, State  # pylint: disable=E0611, E0401
from iplanrio.pipelines_utils.logging import log  # pylint: disable=E0611, E0401

# pylint: disable=E0611, E0401
from pipelines.rj_crm__disparo_template.utils.tasks import (
    download_data_from_bigquery,
)


async def _send_discord_webhook(webhook_url: str, message: str):
    """
    Envia mensagem simples para webhook do Discord.

    Args:
        webhook_url: URL do webhook do Discord
        message: Mensagem de texto a ser enviada
    """
    if len(message) > 2000:
        raise ValueError(f"Message content is too long: {len(message)} > 2000 characters.")

    async with aiohttp.ClientSession() as session:
        webhook = Webhook.from_url(webhook_url, session=session)
        try:
            await webhook.send(content=message)
        except Exception as error:
            raise ValueError(f"Error sending message to Discord webhook: {error}")


def send_discord_notification(webhook_url: str = None, message: str = 'No message provided'):
    """
    Envia uma notificação genérica para um webhook do Discord.

    Args:
        webhook_url: URL do webhook do Discord
        message: Mensagem de texto a ser enviada
    """
    if not webhook_url:
        log(
            "Discord webhook URL not provided. Using default.",
            level="warning",
        )

    try:
        asyncio.run(_send_discord_webhook(webhook_url, message))
        log("Discord notification sent successfully")
    except Exception as error:
        log(f"Failed to send Discord notification: {error}", level="error")


def send_dispatch_no_destinations_found(
    id_hsm: int,
    campaign_name: str,
    cost_center_id: int,
    test_mode: bool = False,
):
    """
    Envia notificação para Discord quando nenhum destinatário é encontrado na query.

    Args:
        id_hsm: ID do template HSM utilizado
        campaign_name: Nome da campanha
        cost_center_id: ID do centro de custo
        test_mode: Indica se é um disparo de teste (opcional)
    """

     # Adicionar indicador [TESTE] no título se test_mode=True
    title = "⚠️ **[TESTE] Disparo não realizado pois nenhum destinatário foi encontrado.**" if test_mode else "⚠️ **Disparo não realizado pois nenhum destinatário foi encontrado.**"

    # Formatar mensagem com contexto e resultados
    message = f"""{title}

📋 **Campanha:** {campaign_name}
🆔 **Template ID:** {id_hsm}
💰 **Centro de Custo:** {cost_center_id}
🕐 **Disparo realizado em:** {datetime.now(timezone("America/Sao_Paulo")).date()}

"""

    webhook_url = os.getenv("DISCORD_WEBHOOK_URL_DISPAROS")
    send_discord_notification(webhook_url, message)


def send_dispatch_success_notification(
    total_dispatches: int,
    dispatch_date: str,
    id_hsm: int,
    campaign_name: str,
    cost_center_id: int,
    total_batches: int,
    sample_destination: dict = None,
    test_mode: bool = False,
    # whitelist_percentage: int = 0,
    attempt_number: int = 1,
    max_dispatch_retries: int = 3,
):
    """
    Envia notificação de sucesso de disparo para Discord.

    Args:
        total_dispatches: Quantidade total de disparos realizados
        dispatch_date: Data/hora do disparo (formato: YYYY-MM-DD HH:MM:SS)
        id_hsm: ID do template HSM utilizado
        campaign_name: Nome da campanha
        cost_center_id: ID do centro de custo
        total_batches: Número total de lotes enviados
        sample_destination: Exemplo de destinatário (opcional)
        test_mode: Indica se é um disparo de teste (opcional)
        attempt_number: Número da tentativa de repescagem
        max_dispatch_retries: Número máximo de tentativas de repescagem
    """

    # Adicionar indicador [TESTE] no título se test_mode=True
    title = "✅ **[TESTE] Disparo Realizado com Sucesso**" if test_mode else "✅ **Disparo Realizado com Sucesso**"

    message = f"""{title}

📊 **Quantidade:** {total_dispatches} disparos
📦 **Lotes:** {total_batches} lotes
🕐 **Hora:** {dispatch_date}
🆔 **ID HSM:** {id_hsm}
📋 **Campanha:** {campaign_name}
💰 **Centro de Custo:** {cost_center_id}
*️⃣ **Tentativa:** {attempt_number} de {max_dispatch_retries}
"""
# 📄 **Porcentagem de pessoas na Whitelist:** {whitelist_percentage}%
# 📖 **Quantidade de pessoas na Whitelist:** {int(whitelist_percentage*total_dispatches/100)}

    # Add sample destination if provided
    if sample_destination:
        message += f"""
📱 **Exemplo de Disparo:**
```json
{_format_sample_destination(sample_destination)}
```
"""
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL_DISPAROS")
    send_discord_notification(webhook_url, message)


def send_dispatch_result_notification(
    total_dispatches: int,
    dispatch_date: str,
    id_hsm: int,
    campaign_name: str,
    cost_center_id: int,
    total_batches: int,
    test_mode: bool = False,
):
    """
    Envia notificação com resultados do disparo após consulta no BigQuery.

    Executa query para verificar status dos disparos realizados e envia
    resumo formatado para Discord.

    Args:
        total_dispatches: Quantidade total de disparos realizados
        dispatch_date: Data/hora do disparo (formato: YYYY-MM-DD HH:MM:SS)
        id_hsm: ID do template HSM utilizado
        campaign_name: Nome da campanha
        cost_center_id: ID do centro de custo
        total_batches: Número total de lotes enviados
        sample_destination: Exemplo de destinatário (opcional, não usado aqui)
        test_mode: Indica se é um disparo de teste (opcional)
    """

    webhook_url = os.getenv("DISCORD_WEBHOOK_URL_DISPAROS")

    # Query para obter resultados do disparo
    results_query = f"""
        WITH ranked_messages AS (
            SELECT
                targetExternalId,
                templateId,
                triggerId,
                status,
                datarelay_timestamp,
                sendDate
            FROM `rj-crm-registry.brutos_wetalkie_staging.fluxo_atendimento_*`
            WHERE templateId = {id_hsm}
                AND sendDate >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 minute)
        ),
        total as (
            select COUNT(DISTINCT targetExternalId) AS total_disparos
            FROM ranked_messages
        ),
        aggregated AS (
            SELECT
                status,
                CASE
                WHEN status = "PROCESSING" THEN 1
                WHEN status = "SENT" THEN 2
                WHEN status = "DELIVERED" THEN 3
                WHEN status = "READ" THEN 4
                WHEN status = "FAILED" THEN 5
                END AS status_ranking,
                COUNT(DISTINCT targetExternalId) AS quantidade_cpfs,
                COUNT(*) AS quantidade_disparos
            FROM ranked_messages
            GROUP BY status, status_ranking
            )
            SELECT
            status,
            quantidade_cpfs,
            quantidade_disparos,
            ROUND(
                quantidade_cpfs * 100.0 / total_disparos,
                1
            ) AS percentual
            FROM aggregated
            cross join total
            ORDER BY status_ranking;
    """

    log("Querying BigQuery for dispatch results...")

    # Executar query no BigQuery
    results_df = download_data_from_bigquery(
        query=results_query, billing_project_id="rj-crm-registry", bucket_name="rj-crm-registry"
    )

    # Adicionar indicador [TESTE] no título se test_mode=True
    title = "📊 **[TESTE] Resultados do Disparo**" if test_mode else "📊 **Resultados do Disparo**"

    # Formatar mensagem com contexto e resultados
    message = f"""{title}

📋 **Campanha:** {campaign_name}
🆔 **Template ID:** {id_hsm}
💰 **Centro de Custo:** {cost_center_id}
🕐 **Disparo realizado em:** {dispatch_date}
📦 **Total enviado:** {total_dispatches} disparos em {total_batches} lotes

**Status dos Disparos:**
"""

    # Adicionar resultados da query formatados
    if len(results_df) > 0:
        for _, row in results_df.iterrows():
            status = str(row["status"])
            cpfs = int(row["quantidade_cpfs"])
            disparos = int(row["quantidade_disparos"])
            percent = float(row["percentual"])

            message += f"• **{status}**: {cpfs} CPFs ({disparos} disparos) - {percent:.1f}%\n"
    else:
        message += "⚠️ Nenhum resultado encontrado ainda.\n"
        message += "Os dados podem ainda não ter sido processados pelo webhook."

    send_discord_notification(webhook_url, message)


def send_retry_dispatch_result_notification(
    total_dispatches: int,
    dispatch_date: str,
    id_hsm: int,
    campaign_name: str,
    cost_center_id: int,
    total_batches: int,
    test_mode: bool = False,
):
    """
    Envia notificação com resultados finais considerando retentativas.
    """
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL_DISPAROS")

    # Query que agrupa por CPF e pega o status mais recente
    results_query = f"""
        WITH ranked_messages AS (
            SELECT
                targetExternalId,
                status,
                ROW_NUMBER() OVER(PARTITION BY targetExternalId ORDER BY datarelay_timestamp DESC) as rn
            FROM `rj-crm-registry.brutos_wetalkie_staging.fluxo_atendimento_*`
            WHERE templateId = {id_hsm}
                AND sendDate >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
        ),
        latest_status AS (
            SELECT status, targetExternalId FROM ranked_messages WHERE rn = 1
        ),
        total AS (
            SELECT COUNT(DISTINCT targetExternalId) as total_cpfs FROM latest_status
        ),
        aggregated AS (
            SELECT
                status,
                CASE
                    WHEN status = "PROCESSING" THEN 1
                    WHEN status = "SENT" THEN 2
                    WHEN status = "DELIVERED" THEN 3
                    WHEN status = "READ" THEN 4
                    WHEN status = "FAILED" THEN 5
                    ELSE 6
                END AS status_ranking,
                COUNT(*) as quantidade_cpfs
            FROM latest_status
            GROUP BY status, status_ranking
        )
        SELECT
            status,
            quantidade_cpfs,
            ROUND(quantidade_cpfs * 100.0 / total_cpfs, 1) as percentual
        FROM aggregated
        CROSS JOIN total
        ORDER BY status_ranking
    """

    log("Querying BigQuery for final retry results...")
    results_df = download_data_from_bigquery(
        query=results_query, billing_project_id="rj-crm-registry", bucket_name="rj-crm-registry"
    )

    title = "🔄 **[TESTE] Resultados Finais (Pós-Retentativa)**" if test_mode else "🔄 **Resultados Finais (Pós-Retentativa)**"

    message = f"""{title}

📋 **Campanha:** {campaign_name}
🆔 **Template ID:** {id_hsm}
🕐 **Início do Disparo:** {dispatch_date}
📦 **Total de CPFs alvo:** {total_dispatches}

**Status Final por CPF:**
"""
    if len(results_df) > 0:
        for _, row in results_df.iterrows():
            message += f"• **{row['status']}**: {int(row['quantidade_cpfs'])} CPFs ({row['percentual']}%)\n"
    else:
        message += "⚠️ Nenhum resultado processado até o momento."

    send_discord_notification(webhook_url, message)


def _format_sample_destination(destination: dict) -> str:
    """
    Formata um destinatário de exemplo para exibição.

    Args:
        destination: Dicionário com dados do destinatário

    Returns:
        String formatada em JSON
    """

    # Create a clean sample with only relevant fields
    sample = {
        "to": destination.get("to", ""),
        "externalId": destination.get("externalId", ""),
        "vars": destination.get("vars", {}),
    }

    return json.dumps(sample, indent=2, ensure_ascii=False)


def send_discord_notification_on_failure(flow: Flow, flow_run: FlowRun, state: State):
    """
    Sends a Discord notification when a flow run fails.
    """
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL_ERRORS")
    if not webhook_url:
        print("DISCORD_WEBHOOK_URL_ERRORS environment variable not set. Cannot send notification.")
        return

    message = f"""
    Prefect flow run failed!
    Flow: {flow.name}
    Flow Run: {flow_run.name}
    State: {state.name}
    Message: {state.message}
    """
    send_discord_notification(webhook_url, message)
