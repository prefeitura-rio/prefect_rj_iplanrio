"""Notificações no Discord (opcionais).

Webhook em ``DISCORD_WEBHOOK_URL_CZRM_SALESFORCE``. Ausente = notificação
pulada com aviso no log. Notificação nunca derruba o flow.
"""

from __future__ import annotations

import os

import requests
from prefect import task
from prefect.client.schemas.objects import FlowRun

from pipelines.rj_iplanrio__czrm_salesforce_datacloud.constants import DISCORD_WEBHOOK_ENV
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.utils.discord import (
    mensagem_falha_flow,
    mensagem_falha_tabela,
    mensagem_resumo,
)
from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)


def _enviar(mensagem: str) -> None:
    url = os.getenv(DISCORD_WEBHOOK_ENV)
    if not url:
        logger.info("[NOTIFY] %s não configurado — notificação pulada.", DISCORD_WEBHOOK_ENV)
        return
    try:
        resposta = requests.post(url, json={"content": mensagem}, timeout=15)
        resposta.raise_for_status()
    except Exception as exc:
        logger.warning("[NOTIFY] falha ao enviar no Discord: %s", exc)


@task(log_prints=True)
def notify_falha_tabela(tabela: str, grupo: str, janela: str, erro: str) -> None:
    """Avisa que uma tabela falhou (o flow segue para as próximas)."""
    _enviar(mensagem_falha_tabela(tabela, grupo, janela, erro))


@task(log_prints=True)
def notify_resumo(
    rotulo: str,
    resultados: dict[str, dict[str, int]],
    falhas: list[str],
    minutos: float,
) -> None:
    """Resumo de fim de run (só nos modos frios e na carga inicial)."""
    _enviar(mensagem_resumo(rotulo, resultados, falhas, minutos))


@task(log_prints=True)
def notify_falha_flow(_flow: object, flow_run: FlowRun, _state: object) -> None:
    """Hook ``on_failure``: avisa que o flow abortou."""
    _enviar(mensagem_falha_flow(flow_run))
