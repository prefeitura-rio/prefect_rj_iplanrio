# -*- coding: utf-8 -*-
# flake8: noqa:E501
# pylint: disable='line-too-long'

"""
Módulo de notificações Discord para pipeline de disparo PIC lembrete.
"""

import asyncio
import json
import os

import aiohttp  # pylint: disable=E0611, E0401
from discord import Webhook  # pylint: disable=E0611, E0401
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


def send_dispatch_success_notification(
    total_dispatches: int,
    dispatch_date: str,
    id_hsm: int,
    campaign_name: str,
    cost_center_id: int,
    total_batches: int,
    sample_destination: dict = None,
    test_mode: bool = False,
    whitelist_percentage: int = 0,
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
    """
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL_DISPAROS")

    if not webhook_url:
        log(
            "Discord webhook URL not configured. Skipping notification.",
            level="warning",
        )
        return

    # Adicionar indicador [TESTE] no título se test_mode=True
    title = "✅ **[TESTE] Disparo Realizado com Sucesso**" if test_mode else "✅ **Disparo Realizado com Sucesso**"

    message = f"""{title}

📊 **Quantidade:** {total_dispatches} disparos
📦 **Lotes:** {total_batches} lotes
🕐 **Hora:** {dispatch_date}
🆔 **ID HSM:** {id_hsm}
📋 **Campanha:** {campaign_name}
💰 **Centro de Custo:** {cost_center_id}
📄 **Porcentagem de pessoas na Whitelist:** {whitelist_percentage}%
📖 **Quantidade de pessoas na Whitelist:** {int(whitelist_percentage*total_dispatches/100)}
"""

    # Add sample destination if provided
    if sample_destination:
        message += f"""
📱 **Exemplo de Disparo:**
```json
{_format_sample_destination(sample_destination)}
```
"""

    try:
        asyncio.run(_send_discord_webhook(webhook_url, message))
        log("Discord notification sent successfully")
    except Exception as error:
        log(f"Failed to send Discord notification: {error}", level="error")


def send_dispatch_result_notification(
    total_dispatches: int,
    dispatch_date: str,
    id_hsm: int,
    campaign_name: str,
    cost_center_id: int,
    total_batches: int,
    sample_destination: dict = None,
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

    if not webhook_url:
        log(
            "Discord webhook URL not configured. Skipping results notification.",
            level="warning",
        )
        return

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

    try:
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

        # Enviar notificação
        asyncio.run(_send_discord_webhook(webhook_url, message))
        log("Discord results notification sent successfully")

    except Exception as error:
        log(f"Failed to send Discord results notification: {error}", level="error")


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
