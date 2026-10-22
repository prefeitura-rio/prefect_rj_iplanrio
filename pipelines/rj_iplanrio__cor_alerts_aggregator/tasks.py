"""Tasks of rj_iplanrio__cor_alerts_aggregator."""

from asyncio import to_thread
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

from google.api_core.exceptions import NotFound
from httpx2 import HTTPError
from prefect import task
from toolkit.telemetry import logger

from pipelines.rj_iplanrio__cor_alerts_aggregator.constants import (
    ALLOWED_NEIGHBORHOODS,
    DATASET_ID,
    HISTORY_TABLE_ID,
    SENT_TABLE_ID,
    SP_TZ,
    TEST_PREFIX,
)
from pipelines.rj_iplanrio__cor_alerts_aggregator.helpers.clusters import build_event, read_alerts
from pipelines.rj_iplanrio__cor_alerts_aggregator.helpers.cor_api import open_event
from pipelines.rj_iplanrio__cor_alerts_aggregator.helpers.sheets import update_sheet
from pipelines.rj_iplanrio__cor_alerts_aggregator.settings import Settings
from pipelines.rj_iplanrio__cor_alerts_aggregator.types import Alert, Cluster, Row, Sheet
from utils.tasks import publish_table_in_bq, read_bq, upload_files_to_gcs, write_records
from utils.types import Cloud, Table
from utils.utils import capture_options, render_query


@task(retries=2, retry_delay_seconds=10)
async def fetch_alerts_task(project: str, window_minutes: int) -> list[Alert]:
    """Read the alerts of the last minutes that no group has sent, from the allowed neighborhoods.

    The sent table is the one of this pipeline. Before its first run the table does not
    exist, and every alert in the window is pending.
    """
    values = {
        "history_table": f"{project}.{DATASET_ID}.{HISTORY_TABLE_ID}",
        "window_minutes": window_minutes,
        "neighborhoods": list(ALLOWED_NEIGHBORHOODS),
    }
    sent_table = f"{project}.{DATASET_ID}.{SENT_TABLE_ID}"

    try:
        frame = await read_bq(
            render_query(__file__, "pending_alerts", sent_table=sent_table, **values), project=project
        )
    except NotFound:
        logger.info("The sent table does not exist yet: table=%s", sent_table)
        frame = await read_bq(render_query(__file__, "pending_alerts", sent_table="", **values), project=project)

    alerts = read_alerts(frame.to_dicts())
    logger.info("Pending alerts read: count=%d", len(alerts))

    return alerts


@task
async def open_event_task(cluster: Cluster) -> str | None:
    """Send a cluster to the COR API as an event, and return its group id, or nothing if the COR refuses it.

    The event type starts with `[TESTE]` until the COR confirms the events.
    """
    settings = Settings()
    group_id = str(uuid4())
    event = build_event(cluster, group_id, datetime.now(SP_TZ), TEST_PREFIX)

    try:
        await open_event(
            settings.COR_EVENTS_BASE_URL,
            settings.COR_EVENTS_USERNAME,
            settings.COR_EVENTS_PASSWORD.get_secret_value(),
            event,
        )
    except (HTTPError, ValueError) as error:
        logger.error("The COR API refused the cluster: cluster=%d error=%s", cluster.cluster_id, error)
        return None

    logger.info("Cluster sent to the COR: cluster=%d group=%s", cluster.cluster_id, group_id)

    return group_id


@task
async def load_rows_task(rows: list[Row], table_id: str, cloud: Cloud) -> None:
    """Add rows to a table of the dataset of the pipeline in BigQuery."""
    table = Table(dataset_id=DATASET_ID, table_id=table_id)

    with TemporaryDirectory() as directory:
        folder = await write_records.fn(rows, Path(directory))
        await upload_files_to_gcs.fn(
            folder,
            table,
            bucket=cloud["bucket"],
            dump_mode="append",
            source_format="json",
            options=capture_options("append"),
        )

    await publish_table_in_bq.fn(table, cloud)
    logger.info("Rows added: table=%s count=%d", table_id, len(rows))


@task
async def write_sheet_task(rows: list[Row], sheet: Sheet) -> int:
    """Upsert the rows of the clusters in the Google Sheet by group id, and drop the stale rows."""
    written = await to_thread(update_sheet, rows, sheet)
    logger.info("Sheet written: rows=%d", written)

    return written
