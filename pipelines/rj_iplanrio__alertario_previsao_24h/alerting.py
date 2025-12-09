# -*- coding: utf-8 -*-
"""
Utilidades para geração e envio de alertas de precipitação no Discord.
"""

from __future__ import annotations

import hashlib
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, datetime
from time import sleep
from typing import Optional, Sequence

import pandas as pd
import requests
from basedosdados import Base
from google.cloud import bigquery

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


def download_data_from_bigquery(
    query: str, billing_project_id: str, bucket_name: str
) -> pd.DataFrame:
    """
    Execute a BigQuery SQL query and return results as a pandas DataFrame.

    Args:
        query (str): SQL query to execute in BigQuery
        billing_project_id (str): GCP project ID for billing purposes
        bucket_name (str): GCS bucket name for credential loading

    Returns:
        pd.DataFrame: Query results as a pandas DataFrame

    Raises:
        Exception: If BigQuery job fails or credentials cannot be loaded
    """
    log("Querying data from BigQuery")
    query = str(query)
    bq_client = bigquery.Client(
        credentials=Base(bucket_name=bucket_name)._load_credentials(mode="prod"),
        project=billing_project_id,
    )
    job = bq_client.query(query)
    while not job.done():
        sleep(1)
    log("Getting result from query")
    results = job.result()
    log("Converting result to pandas dataframe")
    dfr = results.to_dataframe(create_bqstorage_client=False)
    log("End download data from bigquery")
    return dfr


def send_discord_webhook_message(
    webhook_url: str, message: str, timeout: int = 15
) -> dict:
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
        raise ValueError(
            f"Falha ao enviar alerta ao Discord: {response.status_code} - {response.text}"
        )

    try:
        return response.json()
    except ValueError:
        return {}


def check_alert_deduplication(
    alert_hash: str,
    billing_project_id: str,
    max_daily_alerts: int,
    min_alert_interval_hours: int,
) -> tuple[bool, str]:
    """
    Verifica se alerta deve ser enviado com base em regras de deduplicação.

    Consulta tabela alerts_log no BigQuery (hardcoded) e verifica:
    1. Este hash já foi enviado hoje? (duplicata exata)
    2. Já enviamos max_daily_alerts hoje? (limite diário)
    3. Último alerta foi há menos de min_alert_interval_hours? (intervalo)

    Args:
        alert_hash: SHA-256 hash da mensagem de alerta
        billing_project_id: ID do projeto GCP para billing
        max_daily_alerts: Número máximo de alertas por dia
        min_alert_interval_hours: Intervalo mínimo em horas entre alertas

    Returns:
        (should_send, reason) onde:
        - should_send: True se todas as verificações passarem
        - reason: Explicação para logging (por que envia ou pula)

    Raises:
        Exception: Se query do BigQuery falhar (caller deve pegar e não enviar)
    """
    try:
        # Query com tabela hardcoded (padrão do projeto)
        query = f"""
        WITH brazil_today AS (
          SELECT DATE(CURRENT_TIMESTAMP(), 'America/Sao_Paulo') as local_date
        ),
        alert_stats AS (
          SELECT
            -- Conta quantos ALERTAS ÚNICOS têm este hash (usando discord_message_id)
            COUNT(DISTINCT CASE
              WHEN alert_hash = '{alert_hash}' THEN discord_message_id
              END) as hash_count_today,
            -- Conta quantos ALERTAS ÚNICOS foram enviados (mensagens Discord únicas)
            COUNT(DISTINCT discord_message_id) as total_alerts_today,
            -- Timestamp do último alerta (sent_at é STRING, precisa CAST)
            MAX(CAST(sent_at AS TIMESTAMP)) as last_alert_sent_at
          FROM `rj-iplanrio.brutos_alertario_staging.alertario_precipitacao_alerts_log` al
          CROSS JOIN brazil_today bt
          WHERE CAST(al.data_particao AS DATE) = bt.local_date
        )
        SELECT
          hash_count_today,
          total_alerts_today,
          last_alert_sent_at,
          CASE
            WHEN last_alert_sent_at IS NULL THEN NULL
            ELSE TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_alert_sent_at, HOUR)
          END as hours_since_last_alert
        FROM alert_stats
        """

        # Usar função local
        df = download_data_from_bigquery(
            query=query,
            billing_project_id=billing_project_id,
            bucket_name=billing_project_id,
        )

        # Tabela vazia ou primeira execução
        if df.empty or df["total_alerts_today"].iloc[0] == 0:
            return (True, "No alerts sent today. Proceeding with send.")

        row = df.iloc[0]
        hash_count = row["hash_count_today"]
        total_today = row["total_alerts_today"]
        hours_since = row["hours_since_last_alert"]

        # Check 1: Duplicate hash
        if hash_count > 0:
            return (
                False,
                f"Duplicate alert. Hash {alert_hash[:8]}... already sent today.",
            )

        # Check 2: Daily limit
        if total_today >= max_daily_alerts:
            return (
                False,
                f"Daily limit reached. {total_today}/{max_daily_alerts} alerts sent today.",
            )

        # Check 3: Time interval
        if hours_since is not None and hours_since < min_alert_interval_hours:
            return (
                False,
                f"Too soon. {hours_since}h elapsed (minimum {min_alert_interval_hours}h).",
            )

        # All checks passed
        return (
            True,
            f"All checks passed. Sending alert ({total_today + 1}/{max_daily_alerts} today).",
        )

    except Exception as e:
        # Tabela não existe (primeira execução)
        error_msg = str(e).lower()
        if "not found" in error_msg:
            log(f"Alert log table not found (first run): {e}", level="warning")
            return (True, "First run, proceeding with send.")
        # Qualquer outro erro → não envia
        raise
