"""Tasks of rj_iplanrio__alertario_previsao_24h."""

from datetime import datetime
from uuid import uuid4

import polars as pl
from google.api_core.exceptions import NotFound
from httpx2 import AsyncClient
from prefect import task
from toolkit.telemetry import logger

from pipelines.rj_iplanrio__alertario_previsao_24h.constants import (
    ALERT_LOG_TABLE,
    DEFAULT_MAX_DAILY_ALERTS,
    DEFAULT_MIN_INTERVAL_HOURS,
    SP_TZ,
    XML_URL,
)
from pipelines.rj_iplanrio__alertario_previsao_24h.helpers import (
    build_log_rows,
    build_tables,
    decide_alert,
    format_alert_message,
    hash_message,
)
from pipelines.rj_iplanrio__alertario_previsao_24h.types import Alerts, Forecast, PrecipitationAlert, Row
from utils.tasks import notify_discord, read_bq
from utils.utils import capture_time, render_query


@task(retries=3, retry_delay_seconds=10)
async def fetch_forecast_task() -> str:
    """Download the forecast XML of the AlertaRio."""
    async with AsyncClient(timeout=30) as client:
        response = await client.get(XML_URL)
        response.raise_for_status()

    logger.info("Forecast downloaded: bytes=%d", len(response.content))

    return response.text


@task
def build_tables_task(xml: str) -> Forecast:
    """Read the forecast XML into the rows of four tables, with one execution id."""
    forecast = build_tables(xml, str(uuid4()), capture_time())
    logger.info("Forecast read: %s", {name: len(rows) for name, rows in forecast["tables"].items()})

    return forecast


async def read_alert_stats(project: str, full_name: str, alert_hash: str) -> dict[str, int | None] | None:
    """Return the stats of the alert log for the hash, or nothing if the log is new."""
    try:
        frame = await read_bq(
            render_query(__file__, "alert_stats", table=full_name, alert_hash=alert_hash), project=project
        )
    except NotFound:
        logger.info("The alert log does not exist yet: table=%s", full_name)
        return None

    return frame.cast(pl.Int64).to_dicts()[0] if frame.height else None


def decide_to_send(stats: dict[str, int | None] | None, alert_hash: str, rules: Alerts) -> bool:
    """Decide with the rules if the alert goes out, and log the reason."""
    should_send, reason = decide_alert(
        stats,
        alert_hash,
        rules.get("max_daily", DEFAULT_MAX_DAILY_ALERTS),
        rules.get("min_interval_hours", DEFAULT_MIN_INTERVAL_HOURS),
    )
    logger.info("Alert decision: send=%s reason=%s", should_send, reason)

    return should_send


@task
async def alert_task(
    alerts: list[PrecipitationAlert], forecast: Forecast, *, project: str, dataset_id: str, rules: Alerts
) -> list[Row]:
    """Send the alert of heavy rain to Discord, and return the rows for the alert log.

    Nothing is sent, and no row returns, if the same alert went out today, the day has
    the most alerts, or the last alert is too recent. The log table is in `dataset_id`
    of `project`.
    """
    message = format_alert_message(alerts, forecast["synoptic"], forecast["created"].date())
    alert_hash = hash_message(message)
    full_name = f"{project}.{dataset_id}.{ALERT_LOG_TABLE}"

    stats = await read_alert_stats(project, full_name, alert_hash)
    if not decide_to_send(stats, alert_hash, rules):
        return []

    message_id = await notify_discord(message)

    return build_log_rows(
        alerts,
        execution_id=forecast["execution_id"],
        message=message,
        message_id=message_id,
        sent_at=datetime.now(SP_TZ),
    )
