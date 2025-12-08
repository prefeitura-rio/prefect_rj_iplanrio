# -*- coding: utf-8 -*-
"""
Utilidades para geração e envio de alertas de precipitação no Discord.
"""

from __future__ import annotations

import hashlib
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional, Sequence

import pandas as pd
import requests

from iplanrio.pipelines_utils.logging import log

SAFE_PRECIPITATION_VALUES = {"Sem chuva", "Chuva fraca isolada"}


@dataclass(frozen=True)
class PrecipitationAlert:
    """Representa uma combinação data/periodo com precipitação relevante."""

    forecast_date: date
    periodo: str
    precipitacao: str


def extract_precipitation_alerts(dataframe: pd.DataFrame) -> list[PrecipitationAlert]:
    """
    Converte o DataFrame dim_previsao_periodo em uma lista de alertas relevantes.
    """
    if dataframe is None or dataframe.empty:
        return []

    alerts: list[PrecipitationAlert] = []
    for _, row in dataframe.iterrows():
        precipitation = (row.get("precipitacao") or "").strip()
        if not precipitation or precipitation in SAFE_PRECIPITATION_VALUES:
            continue

        forecast_date = row.get("data_periodo")
        if isinstance(forecast_date, str):
            forecast_date = datetime.strptime(forecast_date, "%Y-%m-%d").date()
        elif isinstance(forecast_date, pd.Timestamp):
            forecast_date = forecast_date.date()

        periodo = (row.get("periodo") or "").strip()
        alerts.append(
            PrecipitationAlert(
                forecast_date=forecast_date,
                periodo=periodo,
                precipitacao=precipitation,
            )
        )

    return alerts


def format_precipitation_alert_message(
    alerts: Sequence[PrecipitationAlert],
    synoptic_summary: Optional[str] = None,
    synoptic_reference_date: Optional[date] = None,
) -> str:
    """
    Monta o payload de mensagem seguindo o layout combinado.
    """
    if not alerts:
        raise ValueError("Lista de alertas vazia não pode ser formatada.")

    grouped: OrderedDict[date, list[PrecipitationAlert]] = OrderedDict()
    for alert in alerts:
        grouped.setdefault(alert.forecast_date, []).append(alert)

    synoptic_summary = (synoptic_summary or "").strip()
    lines: list[str] = ["⚠️ Previsão de chuva – próximos dias (AlertaRio)", ""]
    if synoptic_summary:
        synoptic_reference_date = (
            synoptic_reference_date.date()
            if isinstance(synoptic_reference_date, datetime)
            else synoptic_reference_date
        )
        if isinstance(synoptic_reference_date, date):
            synoptic_date_str = synoptic_reference_date.strftime("%d/%m/%Y")
            lines.extend(
                [f"Quadro sinótico – {synoptic_date_str}", synoptic_summary, ""]
            )
        else:
            lines.extend(["Quadro sinótico", synoptic_summary, ""])

    for forecast_date in sorted(grouped):
        items = grouped[forecast_date]
        formatted_date = forecast_date.strftime("%d/%m/%Y")
        lines.append(formatted_date)
        for alert in items:
            lines.append(f"• {alert.periodo or '-'}: {alert.precipitacao}")
        lines.append("")

    return "\n".join(lines).strip()


def compute_message_hash(message: str) -> str:
    """Retorna hash SHA-256 determinístico do corpo enviado ao Discord."""
    return hashlib.sha256(message.encode("utf-8")).hexdigest()




def build_alert_log_rows(
    *,
    alert_date: date,
    id_execucao: str,
    alert_hash: str,
    alerts: Sequence[PrecipitationAlert],
    sent_at: datetime,
    discord_message_id: str | None,
    webhook_channel: str | None,
    message_excerpt: str,
    severity_level: str = "info",
) -> list[dict]:
    """Constrói payload de linhas a serem inseridas no log do BigQuery."""
    truncated_excerpt = message_excerpt[:500]
    rows: list[dict] = []
    for alert in alerts:
        rows.append(
            {
                "alert_date": alert_date,
                "id_execucao": id_execucao,
                "forecast_date": alert.forecast_date,
                "periodo": alert.periodo,
                "precipitacao": alert.precipitacao,
                "alert_hash": alert_hash,
                "severity_level": severity_level,
                "sent_at": sent_at,
                "discord_message_id": discord_message_id,
                "webhook_channel": webhook_channel,
                "message_excerpt": truncated_excerpt,
            }
        )
    return rows




def send_discord_webhook_message(webhook_url: str, message: str, timeout: int = 15) -> dict:
    """
    Envia mensagem para o webhook retornando o payload da resposta (quando disponível).
    """
    if len(message) > 2000:
        raise ValueError(f"Mensagem excede limite de 2000 caracteres: {len(message)}.")

    params = {"wait": "true"}
    response = requests.post(
        webhook_url,
        json={"content": message},
        params=params,
        timeout=timeout,
    )
    if response.status_code not in (200, 204):
        raise ValueError(f"Falha ao enviar alerta ao Discord: {response.status_code} - {response.text}")

    try:
        return response.json()
    except ValueError:
        return {}


