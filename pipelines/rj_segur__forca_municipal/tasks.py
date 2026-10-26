"""Tasks of rj_segur__forca_municipal."""

from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import polars as pl
from prefect import task
from toolkit.telemetry import logger

from pipelines.rj_segur__forca_municipal.constants import (
    DEFAULT_CONCURRENCY,
    DEFAULT_PAGE_SIZE,
    DEFAULT_QMD_ID_CONCURRENCY,
    DEFAULT_UNIT_ID_CONCURRENCY,
    MAX_ITEM_ERROR_RATE,
    SP_TZ,
    TABLE_CONFIG,
    UNIT_IDS_LOOKBACK_PARTITIONS,
)
from pipelines.rj_segur__forca_municipal.helpers.api import FMApi
from pipelines.rj_segur__forca_municipal.helpers.collect import (
    check_error_rate,
    flush_buffer,
    read_details,
    read_kmls,
    read_pages,
    read_unit_days,
)
from pipelines.rj_segur__forca_municipal.helpers.rows import build_frame, cycle_prefix, date_range, parse_date_range
from pipelines.rj_segur__forca_municipal.types import Endpoint, Limits, Row, Tally, Target, Window
from utils.tasks import read_bq, upload_files_to_gcs
from utils.types import Cloud, DumpMode, DumpOptions, Table
from utils.utils import capture_time, render_query


@task
async def read_ids_task(project: str, table: str, name: str, column: str) -> list[str]:
    """Return the sorted ids of a column of a table of the pipeline.

    `name` is `unit_ids` (the units of the latest partitions) or `qmd_ids` (the latest
    QMDs), the query that reads them from `table` of `project`.
    """
    sql = render_query(__file__, name, table=table, partitions=UNIT_IDS_LOOKBACK_PARTITIONS)
    frame = await read_bq(sql, project=project)
    ids: list[str] = frame[column].cast(pl.String).drop_nulls().unique().sort().to_list()
    logger.info("Ids read: query=%s count=%d", name, len(ids))

    return ids


@task
async def upload_cycle_task(rows: list[Row], target: Target, cycle: int) -> None:
    """Load the rows of a cycle to the Delta table as text columns.

    It adds the hash, the time and the date partition.

    Each cycle of a run has its own capture time, so a retry of the run does not repeat
    a cycle.
    """
    frame = build_frame(rows, target.updated_at, target.column)

    with TemporaryDirectory() as directory:
        folder = Path(directory)
        frame.write_parquet(folder / f"{cycle:04d}.parquet")
        options = (
            DumpOptions(capture=capture_time() + timedelta(seconds=cycle))
            if target.dump_mode == "append"
            else DumpOptions()
        )
        await upload_files_to_gcs.fn(
            folder,
            target.table,
            bucket=target.cloud["bucket"],
            dump_mode=target.dump_mode,
            source_format="parquet",
            options=options,
        )

    logger.info("%s %d rows loaded", cycle_prefix(cycle, None), len(rows))


@task
async def run_standard_endpoint_task(table: Table, cloud: Cloud, dump_mode: DumpMode, limits: Limits) -> None:
    """Read every page of a list endpoint and load it in cycles."""
    endpoint = TABLE_CONFIG[table["table_id"]]
    size = limits.get("page_size", DEFAULT_PAGE_SIZE)
    target = Target(table, cloud, dump_mode, datetime.now(SP_TZ).replace(tzinfo=None), "updated_at")

    async def upload(rows: list[Row], cycle: int) -> None:
        await upload_cycle_task(rows, target, cycle)

    async with FMApi.connect(limits.get("concurrency", DEFAULT_CONCURRENCY)) as api:
        first_page = await api.get_first(endpoint, size)

        if not first_page[1] and first_page[0] <= 1:
            logger.warning("The endpoint returned nothing: %s", endpoint)
            return

        await flush_buffer(read_pages(api, endpoint, table["table_id"], size, first_page), upload)


@task
async def run_unit_positions_task(
    table: Table, cloud: Cloud, dump_mode: DumpMode, limits: Limits, window: Window
) -> None:
    """Read the GPS positions of every unit, one day after the other, and load them.

    The days run in order, so a failure leaves the earlier days loaded. The units of a
    day run `unit_id_concurrency` at one time. The units come from the latest partitions
    of `unidades_historico` in BigQuery. A unit that fails is skipped, and the run fails
    if the share of failed unit-days is over 5%.
    """
    first = window.get("data_inicio") or (datetime.now(SP_TZ) - timedelta(days=1)).strftime("%Y-%m-%d")
    first, last = parse_date_range(first, window.get("data_fim") or first)
    unit_ids = await read_ids_task(
        cloud["project"], f"{cloud['project']}.{table['dataset_id']}.unidades_historico", "unit_ids", "UnitId"
    )

    if not unit_ids:
        logger.warning("No unit in the history")
        return

    target = Target(table, cloud, dump_mode, datetime.now(SP_TZ).replace(tzinfo=None), "data_coleta")
    tally = Tally()

    async def upload(rows: list[Row], cycle: int) -> None:
        await upload_cycle_task(rows, target, cycle)

    async with FMApi.connect(limits.get("concurrency", DEFAULT_CONCURRENCY)) as api:
        for number, day in enumerate(date_range(first, last), start=1):
            logger.info("Day %d: %s with %d units", number, day, len(unit_ids))
            endpoint = Endpoint(api, TABLE_CONFIG[table["table_id"]], limits.get("page_size", DEFAULT_PAGE_SIZE))
            batches = read_unit_days(
                endpoint, unit_ids, day, limits.get("unit_id_concurrency", DEFAULT_UNIT_ID_CONCURRENCY), tally
            )

            if not await flush_buffer(batches, upload):
                logger.warning("No position on %s", day)

    check_error_rate("unit_positions", tally, MAX_ITEM_ERROR_RATE)


@task
async def run_qmd_task(table: Table, cloud: Cloud, dump_mode: DumpMode, limits: Limits) -> None:
    """Read the detail or KML of every QMD of the latest partition, and load it.

    `qmd_detalhes` reads `/api/qmd/{id}` and `qmd_kml` reads its KML. The ids run
    `qmd_id_concurrency` at one time. The run fails if the share of failed ids is over
    5%.
    """
    table_id = table["table_id"]
    qmd_ids = await read_ids_task(cloud["project"], f"{cloud['project']}.{table['dataset_id']}.qmd", "qmd_ids", "Id")

    if not qmd_ids:
        logger.warning("No QMD in the catalog")
        return

    target = Target(table, cloud, dump_mode, datetime.now(SP_TZ).replace(tzinfo=None), "updated_at")
    tally = Tally()
    reader = read_details if table_id == "qmd_detalhes" else read_kmls

    async def upload(rows: list[Row], cycle: int) -> None:
        await upload_cycle_task(rows, target, cycle)

    async with FMApi.connect(limits.get("concurrency", DEFAULT_CONCURRENCY)) as api:
        batches = reader(
            api, TABLE_CONFIG[table_id], qmd_ids, limits.get("qmd_id_concurrency", DEFAULT_QMD_ID_CONCURRENCY), tally
        )
        await flush_buffer(batches, upload)

    check_error_rate(table_id, tally, MAX_ITEM_ERROR_RATE)
