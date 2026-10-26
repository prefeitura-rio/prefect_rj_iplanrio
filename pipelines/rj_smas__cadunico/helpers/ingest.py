"""The load of one ZIP of records into the staging table."""

from asyncio import to_thread
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from toolkit.telemetry import logger

from pipelines.rj_smas__cadunico.constants import WORK_FOLDER
from pipelines.rj_smas__cadunico.helpers.prepare import prepare_partition
from pipelines.rj_smas__cadunico.types import RawFolders
from utils.tasks import upload_files_to_gcs
from utils.types import Cloud, Table


async def ingest_archive(name: str, raw: RawFolders, destination: Table, cloud: Cloud) -> None:
    """Download a ZIP, turn it into Parquet files and load them as one day of staging.

    The load is an `append` whose capture is the date of the ZIP, so a rerun adds
    nothing twice.
    """
    with TemporaryDirectory(prefix=WORK_FOLDER) as directory:
        partition = await to_thread(prepare_partition, raw["bucket"], name, Path(directory))

        if partition.date != partition.extracted:
            logger.warning(
                "%s: the date of the name is %s, and the date in the file is %s",
                name,
                partition.date,
                partition.extracted,
            )

        await upload_files_to_gcs.fn(
            partition.folder,
            destination,
            bucket=cloud["bucket"],
            dump_mode="append",
            source_format="parquet",
            options={"capture": datetime.fromisoformat(partition.date)},
        )
        logger.info("Loaded %s: date=%s layout=%s", name, partition.date, partition.layout_version)
