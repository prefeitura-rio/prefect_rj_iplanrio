"""Tasks of rj_iplanrio__sisbicho_images."""

from asyncio import to_thread
from datetime import UTC, datetime

from prefect import task
from toolkit.delta import table_exists
from toolkit.gcs import create_store
from toolkit.telemetry import logger

from pipelines.rj_iplanrio__sisbicho_images.helpers import build_rows, pick_identifier, quote_ids, read_animals
from pipelines.rj_iplanrio__sisbicho_images.types import Row, Storage
from utils.delta import Destination
from utils.tasks import read_bq
from utils.types import Cloud, Table
from utils.utils import render_query


@task(retries=2, retry_delay_seconds=10)
async def list_pending_task(cloud: Cloud, source: Table, target: Table) -> tuple[str, list[str]]:
    """Return the id column of the source table and the ids that the target lacks.

    Only the animals with a QR code or a photo count. The target is the external table
    of the Delta table. Before the first run it does not exist, and every one of them is
    pending.
    """
    project = cloud["project"]
    columns = await read_bq(
        render_query(__file__, "columns", project=project, dataset=source["dataset_id"], table=source["table_id"]),
        project=project,
    )
    identifier = pick_identifier(columns["column_name"].to_list())
    source_table = f"{project}.{source['dataset_id']}.{source['table_id']}"
    destination = Destination(cloud["bucket"], target["dataset_id"], target["table_id"])
    target_table = destination.table(project) if await to_thread(table_exists, destination.uri) else ""

    if not target_table:
        logger.info("The target table does not exist yet: table=%s", destination.table(project))

    frame = await read_bq(
        render_query(
            __file__, "pending_ids", identifier=identifier, source_table=source_table, target_table=target_table
        ),
        project=project,
    )
    ids = quote_ids(frame["animal_identifier"].to_list())
    logger.info("Pending animals: identifier=%s count=%d", identifier, len(ids))

    return identifier, ids


@task
async def process_batch_task(
    project: str, source: Table, identifier: str, ids: list[str], storage: Storage
) -> list[Row]:
    """Read a batch of animals, read each QR code, upload photos, return rows."""
    frame = await read_bq(
        render_query(
            __file__,
            "animals",
            identifier=identifier,
            source_table=f"{project}.{source['dataset_id']}.{source['table_id']}",
            dataset_path=f"{project}.{source['dataset_id']}",
            ids=ids,
        ),
        project=project,
    )
    animals = read_animals(frame.to_dicts())
    rows = await to_thread(build_rows, animals, create_store(storage["bucket"]), storage, datetime.now(UTC))
    logger.info("Batch processed: animals=%d with_photo=%d", len(rows), sum(1 for row in rows if row["foto_url"]))

    return rows
