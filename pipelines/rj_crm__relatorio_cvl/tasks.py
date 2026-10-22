"""Tasks of rj_crm__relatorio_cvl."""

from pathlib import Path

import polars as pl
from prefect import task
from toolkit.telemetry import logger

from pipelines.rj_crm__relatorio_cvl.helpers import build_sessions


@task
def build_sessions_task(messages: pl.DataFrame, report_month: str) -> pl.DataFrame:
    """Build the 24 hour sessions of the messages of a month."""
    sessions = build_sessions(messages, report_month)
    logger.info("Sessions built: month=%s messages=%d", report_month, sessions.height)

    return sessions


@task
def write_sessions_task(sessions: pl.DataFrame, folder: Path) -> Path:
    """Write the sessions as a Parquet file in the folder, and return the folder."""
    sessions.write_parquet(folder / "sessions.parquet")

    return folder
