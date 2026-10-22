"""Tasks of rj_cor__precipitacao_alertario_sftp."""

from asyncio import to_thread

import polars as pl
from google.api_core.exceptions import NotFound
from prefect import task
from toolkit.telemetry import logger

from pipelines.rj_cor__precipitacao_alertario_sftp.helpers import (
    Record,
    download_files,
    drop_repeats,
    list_new_files,
    parse_xml,
)
from utils.tasks import read_bq
from utils.utils import render_query


@task(retries=3, retry_delay_seconds=10)
async def read_watermark_task(table: str, project: str) -> str | None:
    """Return the latest `data_medicao` of the table, or nothing if the table is empty or does not exist."""
    try:
        frame = await read_bq(render_query(__file__, "get_max_update", table=table), project=project)
    except NotFound:
        logger.info("The table does not exist yet: table=%s", table)
        return None

    latest = frame["max_data_medicao"].cast(pl.String).to_list()[0] if frame.height else None
    logger.info("Watermark read: table=%s latest=%s", table, latest)

    return latest


@task(retries=3, retry_delay_seconds=10)
async def fetch_files_task(bucket_name: str, prefix: str, watermark: str | None) -> list[str]:
    """Download the XML files that the table has not seen, and return their text."""
    names = await to_thread(list_new_files, bucket_name, prefix, watermark)
    logger.info("New files: count=%d", len(names))

    return await to_thread(download_files, bucket_name, names)


@task
def build_rows_task(xml_files: list[str]) -> tuple[list[Record], list[Record]]:
    """Read the XML files and return the pluviometric and the meteorological rows, without repeats."""
    pluviometric: list[Record] = []
    meteorological: list[Record] = []

    for xml in xml_files:
        found_pluviometric, found_meteorological = parse_xml(xml)
        pluviometric += found_pluviometric
        meteorological += found_meteorological

    logger.info(
        "Files read: files=%d pluviometric=%d meteorological=%d", len(xml_files), len(pluviometric), len(meteorological)
    )

    return [dict(row) for row in drop_repeats(pluviometric)], [dict(row) for row in drop_repeats(meteorological)]
