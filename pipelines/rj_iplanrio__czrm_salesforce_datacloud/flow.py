"""Flow rj_iplanrio__czrm_salesforce_datacloud.

Synchronises Salesforce Data Cloud (CZRM) tables to BigQuery via GCS Delta Lake.

Two deployments, same code:

- `--staging`: no schedule, `dry_run=True` by default.  Validates extraction
  and transformation against the chosen org without writing anything.
- `--prod`: 15-min, daily and weekly schedules.  Writes to
  `brutos_czrm_salesforce_staging`.

The first load is manual (`modo=carga_inicial`): reads the full table without
a date filter.  Not scheduled on purpose — press the button once.

`origem` selects the credential set (`SF_DC_<ORIGEM>_*`) and the DLO for each
table (`tabelas.yaml`).  A guard prevents non-prod origins from writing to the
production landing zone.

`dry_run=True` still authenticates against the Salesforce org of `origem` to
validate extraction — it skips only the GCS write and BQ publish.

Write path per table (skipped on `dry_run`):
  with TemporaryDirectory():
      write_parquet_records → upload_files_to_gcs  (Parquet → GCS Delta)
  publish_table_in_bq                              (GCS Delta → BigQuery external table)

`publish_table_in_bq` creates the BigQuery table automatically on first use —
no schema creation step is required before writing.
"""

from __future__ import annotations

import time
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal

from prefect import flow
from prefect.logging import get_logger

from pipelines.rj_iplanrio__czrm_salesforce_datacloud.constants import DATASET_ID, DIAS_RECONCILIACAO
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.helpers.janela import build_run_windows
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.helpers.notify import notify_flow_failure
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.helpers.tabelas import NomeTabela, Origem
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.tasks import (
    authenticate_task,
    build_run_label,
    discover_schemas_task,
    notify_run_summary_task,
    process_all_windows_task,
    select_tables,
    utils_table,
    validate_destination,
)
from utils.constants import DEFAULT_CLOUD
from utils.tasks import publish_table_in_bq, upload_files_to_gcs, write_parquet_records
from utils.types import Cloud, DumpMode
from utils.utils import capture_options

logger = get_logger("toolkit")

Modo = Literal["hora", "dia", "reconciliacao", "carga_inicial"]


@flow(
    name="rj_iplanrio__czrm_salesforce_datacloud",
    log_prints=True,
    flow_run_name="{origem}-{modo}",
    on_failure=[notify_flow_failure],
)
async def rj_iplanrio__czrm_salesforce_datacloud(
    origem: Origem = "prod",
    cloud: Cloud = DEFAULT_CLOUD,
    dataset_id: str = DATASET_ID,
    tabelas: list[NomeTabela] | None = None,
    modo: Modo = "hora",
    dia: date | None = None,
    reprocessar: bool = False,
    reprocessar_de: date | None = None,
    reprocessar_ate: date | None = None,
    dump_mode: DumpMode = "append",
    dry_run: bool = False,
    dias_reconciliacao: int = DIAS_RECONCILIACAO,
) -> None:
    """Sync tables from `tabelas.yaml` out of Salesforce Data Cloud into GCS + BigQuery.

    - `origem`: Salesforce org to read from (`qa`, `staging` or `prod`).
    - `cloud`: GCS bucket and BigQuery project.  Default: `DEFAULT_CLOUD`.
    - `dataset_id`: destination BigQuery dataset.  Default: `brutos_czrm_salesforce_staging`.
    - `tabelas`: subset of tables to process.  Empty/None = all.
    - `modo`: `hora` (1-h window), `dia` (full day), `reconciliacao` (last N
      days) or `carga_inicial` (full table, no date filter).
    - `dia`: day to process in `dia` mode.  Default: yesterday.
    - `reprocessar`: reprocess day by day from `reprocessar_de` to `reprocessar_ate`.
    - `reprocessar_de`: first day of the reprocess range (inclusive).
    - `reprocessar_ate`: last day (inclusive).  Default: same as `reprocessar_de`.
    - `dump_mode`: GCS dump mode (`append` or `overwrite`).  Default: `append`.
    - `dry_run`: extract and transform without writing to GCS or BigQuery.
    - `dias_reconciliacao`: lookback window for `reconciliacao` mode.  Default: 7.

    Raise `ValueError` if the origin tries to write to the production landing
    zone or if the table selection is invalid.
    Raise `RuntimeError` if all selected tables fail in every window.
    """
    validate_destination(origem, dataset_id, dry_run)
    start_time = time.monotonic()

    selected = select_tables(list(tabelas) if tabelas else None)
    windows = build_run_windows(
        modo=modo,
        dia=dia,
        reprocessar=reprocessar,
        reprocessar_de=reprocessar_de,
        reprocessar_ate=reprocessar_ate,
        dias_reconciliacao=dias_reconciliacao,
    )
    label = build_run_label(modo, reprocessar, reprocessar_de, reprocessar_ate)

    if reprocessar and modo == "reconciliacao":
        logger.warning("Run mode overridden: reprocessar=True replaces modo=reconciliacao with day-by-day.")

    session = await authenticate_task(origem)

    logger.info(
        "Run started: label=%s origin=%s bucket=%s dataset=%s.%s tables=%s windows=%d",
        label,
        origem,
        cloud["bucket"],
        cloud["project"],
        dataset_id,
        [t.nome for t in selected],
        len(windows),
    )
    if dry_run:
        logger.info("Run mode: dry_run — GCS upload and BigQuery publish skipped.")

    schemas = await discover_schemas_task(session=session, selected=selected, origin=origem)

    frames, failures = await process_all_windows_task(
        windows=windows,
        selected=selected,
        schemas=schemas,
        session=session,
        origin=origem,
        dry_run=dry_run,
    )

    successes = set(frames)
    if not successes:
        raise RuntimeError(f"All tables failed: {', '.join(sorted(t.nome for t in selected))}")

    if not dry_run:
        for table in selected:
            rows = frames.get(table.nome)
            if rows is None or rows.is_empty():
                continue

            with TemporaryDirectory() as directory:
                folder = await write_parquet_records(rows.to_dicts(), Path(directory))
                await upload_files_to_gcs(
                    folder,
                    utils_table(table, dataset_id),
                    bucket=cloud["bucket"],
                    dump_mode=dump_mode,
                    source_format="parquet",
                    options=capture_options(dump_mode),
                )

            await publish_table_in_bq(utils_table(table, dataset_id), cloud)

    elapsed_minutes = (time.monotonic() - start_time) / 60
    total_rows = sum(len(df) for df in frames.values())

    logger.info(
        "Run complete: rows=%d tables=%d elapsed=%.1fmin",
        total_rows,
        len(successes),
        elapsed_minutes,
    )
    if failures:
        logger.warning("Run finished with failures: tables=%s", failures)

    # if not dry_run and (reprocessar or modo in ("dia", "reconciliacao", "carga_inicial")):
    #     await notify_run_summary_task(
    #         label=label,
    #         results={t.nome: len(frames[t.nome]) for t in selected if t.nome in frames},
    #         failures=failures,
    #         minutes=elapsed_minutes,
    #     )
