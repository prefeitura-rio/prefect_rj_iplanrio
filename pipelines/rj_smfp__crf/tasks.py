"""Tasks of rj_smfp__crf."""

from asyncio import to_thread
from datetime import date
from pathlib import Path

import polars as pl
from duckdb import BinderException
from google.api_core.exceptions import NotFound
from prefect import task
from toolkit.telemetry import logger

from pipelines.rj_smfp__crf.helpers import list_names, parse_date, process_zip, select_zip_names
from utils.tasks import read_bq
from utils.utils import render_query


@task(retries=3, retry_delay_seconds=10)
async def read_watermark_task(table: str, project: str) -> date | None:
    """Return the latest `data_particao` of the table, or nothing if it is empty.

    Nothing also means that the table does not exist yet. The ZIP files after that day
    are the ones to load.
    """
    try:
        frame = await read_bq(render_query(__file__, "get_max_partition", table=table), project=project)
    except (NotFound, BinderException) as error:
        if isinstance(error, BinderException) and "Not found" not in str(error):
            raise
        logger.info("The table does not exist yet: table=%s", table)
        return None

    latest = frame["max_data_particao"].cast(pl.String).to_list()[0] if frame.height else None
    logger.info("Watermark read: table=%s latest=%s", table, latest)

    return parse_date(latest, "max_data_particao")


@task
async def process_zip_files_task(
    bucket: str, prefix: str, table_id: str, folder: Path, window: tuple[date | None, date | None]
) -> int:
    """Read the ZIP files of the window into Parquet files of the folder.

    The window is `(start, end]`: a file on `start` or before it was loaded already.
    Return how many ZIP files were read, which is `0` if none is in the window.
    """
    names = await to_thread(list_names, bucket, prefix)
    chosen = select_zip_names(names, *window)
    logger.info("ZIP files: found=%d chosen=%d window=%s", len(names), len(chosen), window)
    total = 0

    for name in chosen:
        rows = await to_thread(process_zip, bucket, name, table_id, folder)
        total += rows
        logger.info("ZIP file read: name=%s rows=%d", name, rows)

    logger.info("ZIP files read: table=%s files=%d rows=%d", table_id, len(chosen), total)

    return len(chosen)
