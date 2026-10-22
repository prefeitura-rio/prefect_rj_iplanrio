"""Flow for rj_iplanrio__sisbicho_images."""

from pathlib import Path
from tempfile import TemporaryDirectory

from prefect import flow
from toolkit.telemetry import logger

from pipelines.rj_iplanrio__sisbicho_images.constants import DEFAULT_BATCH_SIZE
from pipelines.rj_iplanrio__sisbicho_images.tasks import list_pending_task, process_batch_task
from pipelines.rj_iplanrio__sisbicho_images.types import Limits, Row, Storage
from utils.hooks import notify_failure
from utils.tasks import publish_table_in_bq, upload_files_to_gcs, write_records
from utils.types import Cloud, DumpMode, Table
from utils.utils import capture_options


@flow(log_prints=True, flow_run_name="sisbicho-images-{table[table_id]}", on_failure=[notify_failure])
async def rj_iplanrio__sisbicho_images(
    *,
    cloud: Cloud,
    source: Table,
    table: Table,
    storage: Storage,
    limits: Limits | None = None,
) -> None:
    """Read the QR code and upload the photo of each new animal of the SISBICHO, and load them to BigQuery.

    An animal is new when it has a QR code or a photo and its id is not in `table` yet. The animals
    go in batches of `limits["batch_size"]` (1000), at most `limits["max_records"]` in a run. The
    photos go to the bucket of `storage` as the batches run, and one load adds the rows at the end.
    The rows have the id, the CPF of the owner, the QR code payload as JSON and the URL of the photo.
    """
    limits = limits or {}
    size = limits.get("batch_size", DEFAULT_BATCH_SIZE)
    dump_mode: DumpMode = "append"
    identifier, ids = await list_pending_task(cloud["project"], source, table)
    ids = ids[: limits["max_records"]] if "max_records" in limits else ids

    if not ids:
        logger.info("No new animal")
        return

    rows: list[Row] = []

    for start in range(0, len(ids), size):
        rows += await process_batch_task(cloud["project"], source, identifier, ids[start : start + size], storage)

    with TemporaryDirectory() as directory:
        folder = await write_records(rows, Path(directory))
        await upload_files_to_gcs(
            folder,
            table,
            bucket=cloud["bucket"],
            dump_mode=dump_mode,
            source_format="json",
            options=capture_options(dump_mode),
        )

    await publish_table_in_bq(table, cloud)
