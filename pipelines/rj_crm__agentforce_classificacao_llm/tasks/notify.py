"""
Notificação Discord de resumo/falha da pipeline — mesmo padrão de
pipelines/rj_crm__disparo_template/utils/discord.py (webhook simples via discord.py + aiohttp).

Variáveis de ambiente (``NoticeSettings``, via Kubernetes Secret):
  DISCORD_WEBHOOK_URL        : resumo diário (sucesso/parcial)
  DISCORD_ERRORS_WEBHOOK_URL : falha crítica do flow
"""

import asyncio

import aiohttp
from discord import Webhook
from prefect import task
from prefect.client.schemas.objects import Flow, FlowRun, State

from utils.settings import NoticeSettings


async def _send_discord_webhook(webhook_url: str, message: str) -> None:
    async with aiohttp.ClientSession() as session:
        webhook = Webhook.from_url(webhook_url, session=session)
        await webhook.send(content=message)


def _send(webhook_url: str | None, message: str) -> None:
    if not webhook_url:
        print("[NOTIFY] Webhook do Discord não configurado — notificação pulada.")
        return
    try:
        asyncio.run(_send_discord_webhook(webhook_url, message))
        print("[NOTIFY] Notificação Discord enviada.")
    except Exception as exc:  # notificação nunca deve derrubar o flow
        print(f"[NOTIFY] WARN: falha ao enviar notificação Discord: {exc}")


@task(log_prints=True)
def notify_resumo(
    n_extraidas: int,
    n_pre_classificadas: int,
    n_classificadas_llm: int,
    n_falhas: int,
    linhas_carregadas: int,
) -> None:
    icone = "✅" if n_falhas == 0 else "⚠️"
    message = f"""{icone} **agentforce-classificacao-llm: concluído**

📥 **Extraídas (pendentes):** {n_extraidas}
📏 **Pré-classificadas por regra:** {n_pre_classificadas}
🤖 **Classificadas por LLM:** {n_classificadas_llm}
❌ **Falhas (retry automático amanhã):** {n_falhas}
📊 **Linhas carregadas no BQ:** {linhas_carregadas}
"""
    _send(NoticeSettings().DISCORD_WEBHOOK_URL.get_secret_value(), message)


def notify_falha_flow(flow: Flow, flow_run: FlowRun, state: State) -> None:
    """Hook de `on_failure` do flow — assinatura fixa exigida pelo Prefect
    (ver pipelines/rj_crm__disparo_template/flow.py para o mesmo padrão)."""
    message = f"""🚨 **agentforce-classificacao-llm: FALHOU**

Flow: {flow.name}
Flow Run: {flow_run.name}
Mensagem: {state.message}
"""
    _send(NoticeSettings().DISCORD_ERRORS_WEBHOOK_URL.get_secret_value(), message)
