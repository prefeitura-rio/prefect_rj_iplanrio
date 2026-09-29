# -*- coding: utf-8 -*-
"""
Notificações no Discord da pipeline Salesforce Data Cloud → BigQuery.

Webhooks (env var, via secret do work pool — mesmo secret do
rj_crm__agentforce_classificacao_llm):
  DISCORD_WEBHOOK_URL_ERRORS              : falhas — tabela não-crítica que
                                            falhou e flow que abortou (canal de
                                            erro já usado por outras pipelines)
  DISCORD_WEBHOOK_URL_SALESFORCE_DATACLOUD: resumo de fim de run — só modo='dia'
                                            e reprocessamento (o modo 'hora' roda
                                            96x/dia, resumo lá seria spam)
Webhook não configurado = notificação pulada com aviso no log. Notificação
nunca derruba o flow.
"""

from __future__ import annotations

import os

import requests
from prefect import task
from prefect.client.schemas.objects import Flow, FlowRun, State

from pipelines.rj_crm__salesforce_datacloud.utils.discord import (
    mensagem_falha_flow,
    mensagem_falha_tabela,
    mensagem_resumo,
)

_ENV_ERROS = "DISCORD_WEBHOOK_URL_ERRORS"
_ENV_RESUMO = "DISCORD_WEBHOOK_URL_SALESFORCE_DATACLOUD"


def _enviar(env_var: str, mensagem: str) -> None:
    url = os.getenv(env_var)
    if not url:
        print(f"[NOTIFY] {env_var} não configurado — notificação pulada.")
        return
    try:
        resp = requests.post(url, json={"content": mensagem}, timeout=15)
        resp.raise_for_status()
        print("[NOTIFY] Notificação Discord enviada.")
    except Exception as exc:  # notificação nunca deve derrubar o flow
        print(f"[NOTIFY] WARN: falha ao enviar notificação Discord: {exc}")


@task(log_prints=True)
def notify_falha_tabela(tabela: str, grupo: str, janela: str, erro: str) -> None:
    """Tabela não-crítica falhou (a crítica aborta o flow → notify_falha_flow)."""
    _enviar(_ENV_ERROS, mensagem_falha_tabela(tabela, grupo, janela, erro))


@task(log_prints=True)
def notify_resumo(
    rotulo: str,
    resultados: dict[str, dict[str, int]],
    falhas: list[str],
    minutos: float,
) -> None:
    """Resumo de fim de run (o flow só chama em modo='dia' e reprocessamento)."""
    _enviar(_ENV_RESUMO, mensagem_resumo(rotulo, resultados, falhas, minutos))


def notify_falha_flow(flow: Flow, flow_run: FlowRun, state: State) -> None:
    """Hook `on_failure` do flow — assinatura fixa exigida pelo Prefect. Cobre
    tabela crítica e qualquer erro fora do loop de tabelas (auth, BigQuery)."""
    _enviar(_ENV_ERROS, mensagem_falha_flow(flow_run.name, state.message))
