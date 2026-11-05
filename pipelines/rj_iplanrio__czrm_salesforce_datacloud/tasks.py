"""Tasks for rj_iplanrio__czrm_salesforce_datacloud.

Each task is a thin wrapper that delegates to `helpers/` or encapsulates one
unit of orchestration work.  Business logic lives in `helpers/`; retry
policies, Prefect tracking and isolation boundaries live here.

Task hierarchy in a typical run:

    authenticate_task
    discover_schemas_task          (one call per table, sequential)
    process_all_windows_task
      └─ process_table_task        (per window × table, concurrent via gather)
           └─ extract_table_task
    [flow] for table, rows in results:
        with TemporaryDirectory():
            write_parquet_records  (utils.tasks)
            upload_files_to_gcs    (utils.tasks)
        publish_table_in_bq        (utils.tasks)
    notify_run_summary_task        (cold modes and initial load only)
"""

from __future__ import annotations

from asyncio import gather
from dataclasses import dataclass
from datetime import date

import polars as pl
from prefect import task
from prefect.logging import get_logger

from pipelines.rj_iplanrio__czrm_salesforce_datacloud.constants import DATASET_ID
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.helpers import auth as auth_helpers
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.helpers import extract as extract_helpers
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.helpers import notify as notify_helpers
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.helpers.auth import Session, refresh_if_needed
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.helpers.janela import (
    build_filter_clause,
    format_window,
)
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.helpers.retry import is_transient_error
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.helpers.schema import TableSchema, build_schema
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.helpers.tabelas import TABLES, Table
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.helpers.transform import apply_transform
from utils.types import Cloud, DumpMode
from utils.types import Table as UtilsTable

logger = get_logger("toolkit")


# ---------------------------------------------------------------------------
# Types shared between tasks.py and flow.py
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Destination:
    """Write target for one run (cloud config, dataset, dry_run flag)."""

    cloud: Cloud
    dataset_id: str
    dump_mode: DumpMode
    dry_run: bool


# ---------------------------------------------------------------------------
# Guard-rails (pure helpers promoted here because flow.py uses them directly)
# ---------------------------------------------------------------------------


def validate_destination(origin: str, dataset_id: str, dry_run: bool) -> None:
    """Prevent non-prod origins from writing to the production landing zone.

    Raise `ValueError` if the combination is unsafe.
    """
    if origin != "prod" and dataset_id == DATASET_ID and not dry_run:
        raise ValueError(
            f"Origin '{origin}' cannot write to '{DATASET_ID}' (production landing zone). "
            "Use `dry_run=True` or an explicit `dataset_id` for validation."
        )


def select_tables(table_names: list[str] | None) -> list[Table]:
    """Return tables in YAML order, optionally filtered by `table_names`.

    Raise `ValueError` if any name is unknown.
    """
    unknown = set(table_names or []) - {t.nome for t in TABLES}
    if unknown:
        raise ValueError(f"Unknown tables: {sorted(unknown)}. See tabelas.yaml.")
    return [t for t in TABLES if not table_names or t.nome in table_names]


def build_run_label(
    modo: str,
    reprocessar: bool,
    reprocessar_de: date | None,
    reprocessar_ate: date | None,
) -> str:
    """Return a human-readable label for log and notification messages."""
    if reprocessar:
        return f"reprocess {reprocessar_de} to {reprocessar_ate or reprocessar_de}"
    if modo == "carga_inicial":
        return "initial load"
    return f"modo={modo}"


def utils_table(table: Table, dataset_id: str) -> UtilsTable:
    """Convert a pipeline `Table` to the `utils.types.Table` TypedDict."""
    return UtilsTable(dataset_id=dataset_id, table_id=table.nome)


# ---------------------------------------------------------------------------
# Individual tasks
# ---------------------------------------------------------------------------


@task(retries=3, retry_delay_seconds=[30, 60, 120], retry_condition_fn=is_transient_error)
async def authenticate_task(origin: str) -> Session:
    """Authenticate against the Salesforce org and return a session."""
    return await auth_helpers.authenticate(origin)


@task(retries=3, retry_delay_seconds=[30, 60, 120], retry_condition_fn=is_transient_error)
async def discover_columns_task(session: Session, dlo: str, table: str = "") -> list[dict]:
    """Discover column names and types from the DLO via `SELECT * LIMIT 0`."""
    return await extract_helpers.discover_columns(session=session, dlo=dlo, table=table)


@task
async def discover_schemas_task(session: Session, selected: list[Table], origin: str) -> dict[str, TableSchema]:
    """Discover the schema of every selected table from their DLOs.

    Returns a mapping of table name to `TableSchema`.
    """
    schemas: dict[str, TableSchema] = {}
    for table in selected:
        columns = await discover_columns_task(session=session, dlo=table.dlo_for(origin), table=table.nome)
        schemas[table.nome] = build_schema(columns, table.renomear, table.booleanas)
    return schemas


@task(retries=3, retry_delay_seconds=[30, 60, 120], retry_condition_fn=is_transient_error)
async def extract_table_task(
    session: Session,
    dlo: str,
    filter_clause: str,
    order_by: str,
    table: str = "",
) -> pl.DataFrame:
    """Extract all rows from a DLO for the given filter window."""
    return await extract_helpers.extract_table(
        session=session,
        dlo=dlo,
        filter_clause=filter_clause,
        order_by=order_by,
        table=table,
    )


@task
async def process_table_task(
    table: Table,
    schema: TableSchema,
    session: Session,
    origin: str,
    dry_run: bool,
    filter_clause: str,
) -> pl.DataFrame:
    """Extract and transform ONE table for ONE window.

    Returns the transformed DataFrame (may be empty).
    The caller is responsible for writing to GCS and publishing to BigQuery.

    Warns for `carga_inicial` with an empty result — this almost certainly
    indicates the DLO is empty or the credentials are wrong.
    """
    df = await extract_table_task(
        session=session,
        dlo=table.dlo_for(origin),
        filter_clause=filter_clause,
        order_by=table.order_by,
        table=table.nome,
    )

    if df.is_empty():
        if not filter_clause:
            logger.warning(
                "Extraction returned empty: table=%s mode=initial_load — DLO empty or wrong credentials?",
                table.nome,
            )
        return df

    df = apply_transform(df, table=table, schema=schema)

    logger.info("Extraction complete: table=%s rows=%d", table.nome, len(df))
    if dry_run:
        logger.info("Upload skipped: table=%s reason=dry_run", table.nome)
    return df


async def _process_one_table(
    table: Table,
    schema: TableSchema,
    session: Session,
    origin: str,
    dry_run: bool,
    filter_clause: str,
    window_label: str,
) -> tuple[str, pl.DataFrame | None]:
    """Run `process_table_task` for a single table, capturing failures.

    Returns `(table_name, dataframe_or_None)`.  None signals a failure.
    """
    try:
        df = await process_table_task(
            table=table,
            schema=schema,
            session=session,
            origin=origin,
            dry_run=dry_run,
            filter_clause=filter_clause,
        )
        return table.nome, df
    except Exception as exc:
        logger.warning("Table extraction failed: table=%s error=%s", table.nome, exc)
        await notify_table_failure_task(
            table=table.nome,
            group=table.grupo,
            window=window_label,
            error=str(exc),
        )
        return table.nome, None


@task
async def process_all_windows_task(
    windows: list[tuple[str | None, str | None]],
    selected: list[Table],
    schemas: dict[str, TableSchema],
    session: Session,
    origin: str,
    dry_run: bool,
) -> tuple[dict[str, pl.DataFrame], list[str]]:
    """Extract and transform all windows × tables, isolating failures per table.

    Tables within each window are processed concurrently via `asyncio.gather`.
    The token is refreshed before each window to handle long reprocess runs
    that exceed the 2-hour OAuth TTL.

    Returns `(frames, failures)`:
    - `frames`   — `{table_name: concatenated_dataframe}` for every table that
                   succeeded in at least one window.  Rows from all windows are
                   concatenated so the caller writes one Parquet file per table.
    - `failures` — names of tables that failed in at least one window.

    A failure in one table does not prevent the others from running.
    """
    frames: dict[str, list[pl.DataFrame]] = {t.nome: [] for t in selected}
    failures: list[str] = []
    total_windows = len(windows)

    for window_idx, (start, end) in enumerate(windows, start=1):
        window_label = format_window(start, end) if start and end else "initial load"

        if start and end:
            logger.info("Window started: %d/%d %s", window_idx, total_windows, window_label)

        session = await refresh_if_needed(session, origin)

        outcomes = await gather(
            *(
                _process_one_table(
                    table=table,
                    schema=schemas[table.nome],
                    session=session,
                    origin=origin,
                    dry_run=dry_run,
                    filter_clause=build_filter_clause(table.coluna_modificacao, start or "", end or ""),
                    window_label=window_label,
                )
                for table in selected
            )
        )

        for nome, df in outcomes:
            if df is None:
                if nome not in failures:
                    failures.append(nome)
            elif not df.is_empty():
                frames[nome].append(df)

    result: dict[str, pl.DataFrame] = {
        nome: pl.concat(dfs)
        for nome, dfs in frames.items()
        if dfs
    }
    return result, failures


@task
async def notify_table_failure_task(table: str, group: str, window: str, error: str) -> None:
    """Send a Discord failure notification for a single table."""
    await notify_helpers.notify_table_failure(table=table, group=group, window=window, error=error)


@task
async def notify_run_summary_task(
    label: str,
    results: dict[str, int],
    failures: list[str],
    minutes: float,
) -> None:
    """Send the end-of-run summary to Discord."""
    await notify_helpers.notify_run_summary(label=label, results=results, failures=failures, minutes=minutes)
