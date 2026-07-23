# -*- coding: utf-8 -*-
"""
Notificações Slack/Teams para a pipeline Agentforce → BigQuery.

Envia webhook com resumo da execução ao final de cada fase e do flow completo.
Em caso de falha, envia alerta imediato com detalhes do erro.

Credencial esperada:
  SLACK_WEBHOOK_URL : URL do webhook Slack (env var, via Infisical path /agentforce_notifications)
  (opcional) TEAMS_WEBHOOK_URL : URL do webhook MS Teams
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

import requests
from prefect import task


def _send_webhook(url: str, payload: dict) -> bool:
    """Envia payload JSON para um webhook. Retorna True se OK."""
    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        return True
    except Exception as exc:
        print(f"[NOTIFY] WARN: falha ao enviar webhook: {exc}")
        return False


def _build_slack_message(
    title: str,
    status: str,
    details: dict[str, str | int] | None = None,
    error: str | None = None,
) -> dict:
    """Monta payload Slack com blocos formatados."""
    icon = ":white_check_mark:" if status == "ok" else ":x:"
    color = "#36a64f" if status == "ok" else "#ff0000"

    text = f"{icon} *{title}*"
    fields = []

    if details:
        for k, v in details.items():
            fields.append({"type": "mrkdwn", "text": f"*{k}:* {v}"})

    if error:
        fields.append({"type": "mrkdwn", "text": f"*Erro:* ```{error[:500]}```"})

    return {
        "attachments": [
            {
                "color": color,
                "blocks": [
                    {"type": "section", "text": {"type": "mrkdwn", "text": text}},
                    *(
                        [{"type": "section", "fields": fields}]
                        if fields
                        else []
                    ),
                    {
                        "type": "context",
                        "elements": [
                            {
                                "type": "mrkdwn",
                                "text": f"Pipeline agentforce-bq | {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
                            }
                        ],
                    },
                ],
            }
        ]
    }


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task(log_prints=True)
def notify_phase_success(
    phase_name: str,
    rows_by_table: dict[str, int],
    elapsed_seconds: float | None = None,
) -> None:
    """
    Notifica sucesso de uma fase.

    Args:
        phase_name    : Nome da fase (ex: 'F1 — STDM').
        rows_by_table : Dict {tabela: qtd_linhas} para resumo.
        elapsed_seconds: Tempo de execução da fase.
    """
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        print(f"[NOTIFY] SLACK_WEBHOOK_URL nao configurada — notificacao pulada.")
        return

    total_rows = sum(rows_by_table.values())
    details = {t: f"{n:,} linhas" for t, n in rows_by_table.items()}
    if elapsed_seconds:
        details["Tempo"] = f"{elapsed_seconds:.0f}s"
    details["Total"] = f"{total_rows:,} linhas"

    payload = _build_slack_message(
        title=f"agentforce-daily | {phase_name}: OK",
        status="ok",
        details=details,
    )

    print(f"[NOTIFY] Enviando notificacao de sucesso: {phase_name}")
    _send_webhook(webhook_url, payload)


@task(log_prints=True)
def notify_phase_failure(
    phase_name: str,
    error_message: str,
    retry_count: int = 0,
) -> None:
    """
    Notifica falha de uma fase.

    Args:
        phase_name    : Nome da fase.
        error_message : Mensagem de erro.
        retry_count   : Número de tentativas já realizadas.
    """
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        print(f"[NOTIFY] SLACK_WEBHOOK_URL nao configurada — notificacao pulada.")
        return

    details = {"Tentativas": str(retry_count)}
    payload = _build_slack_message(
        title=f"agentforce-daily | {phase_name}: FALHOU",
        status="error",
        details=details,
        error=error_message,
    )

    print(f"[NOTIFY] Enviando notificacao de falha: {phase_name}")
    _send_webhook(webhook_url, payload)


@task(log_prints=True)
def notify_pipeline_summary(
    phase_results: dict[str, dict],
    total_elapsed_seconds: float | None = None,
) -> None:
    """
    Envia resumo final da execução completa do pipeline.

    Args:
        phase_results       : Dict {fase: {tabela: linhas}} por fase.
        total_elapsed_seconds: Tempo total de execução.
    """
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        print(f"[NOTIFY] SLACK_WEBHOOK_URL nao configurada — notificacao pulada.")
        return

    grand_total = sum(
        n for phase in phase_results.values() for n in phase.values()
    )

    details: dict[str, str | int] = {}
    for phase, tables in phase_results.items():
        phase_total = sum(tables.values())
        details[phase] = f"{phase_total:,} linhas"

    if total_elapsed_seconds:
        details["Tempo total"] = f"{total_elapsed_seconds / 60:.1f}min"
    details["Grand total"] = f"{grand_total:,} linhas"

    payload = _build_slack_message(
        title="agentforce-daily: CONCLUIDO",
        status="ok",
        details=details,
    )

    print("[NOTIFY] Enviando resumo final do pipeline.")
    _send_webhook(webhook_url, payload)
