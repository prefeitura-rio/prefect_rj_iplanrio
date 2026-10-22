"""Tasks of rj_iplanrio__eai_history."""

from datetime import datetime

import polars as pl
from google.api_core.exceptions import NotFound
from prefect import task
from toolkit.telemetry import logger

from pipelines.rj_iplanrio__eai_history.constants import (
    DEFAULT_LAST_UPDATE,
    DEFAULT_SESSION_TIMEOUT_SECONDS,
)
from pipelines.rj_iplanrio__eai_history.helpers.history import build_rows, connect, load_threads
from pipelines.rj_iplanrio__eai_history.types import Environment, Options, Row
from utils.tasks import read_bq
from utils.types import Table
from utils.utils import render_query


@task(retries=2, retry_delay_seconds=10)
async def read_last_update_task(project: str, table: Table, environment: Environment) -> str:
    """Return the latest `last_update` of the environment in the table, or the start of time if there is none.

    The start of time also covers a table that does not exist yet, so the first run reads every thread.
    """
    name = f"{project}.{table['dataset_id']}.{table['table_id']}"

    try:
        frame = await read_bq(
            render_query(__file__, "last_update", table=name, environment=environment), project=project
        )
    except NotFound:
        logger.info("The table does not exist yet: table=%s", name)
        return DEFAULT_LAST_UPDATE

    latest = frame["last_update"].cast(pl.String).drop_nulls().to_list()
    logger.info("Latest last_update read: table=%s latest=%s", name, latest[0] if latest else None)

    return latest[0] if latest else DEFAULT_LAST_UPDATE


@task(retries=2, retry_delay_seconds=30)
async def fetch_history_task(last_update: str, environment: Environment, options: Options) -> list[Row]:
    """Read the threads that changed after `last_update`, and return one row for each in the format of the gateway.

    `last_update` must be an ISO time, and it is checked before it reaches the query.
    Raise `ValueError` if it is not.
    """
    cursor = datetime.fromisoformat(last_update).isoformat()
    sql = render_query(__file__, "threads", last_update=cursor, sql_limit=options.get("sql_limit", 0))
    engine, saver = await connect()
    threads = await load_threads(engine, sql)

    if not threads:
        logger.info("No thread changed after %s", cursor)
        return []

    rows = build_rows(
        saver.serde,
        threads,
        environment,
        options.get("session_timeout_seconds", DEFAULT_SESSION_TIMEOUT_SECONDS),
        whatsapp=options.get("use_whatsapp_format", False),
    )
    logger.info("History read: threads=%d rows=%d", len(threads), len(rows))

    return rows
