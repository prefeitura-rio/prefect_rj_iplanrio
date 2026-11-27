"""The load of one ZIP of records into the staging table."""

from asyncio import to_thread
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic

from toolkit.telemetry import logger

from pipelines.rj_smas__cadunico.constants import WORK_FOLDER
from pipelines.rj_smas__cadunico.helpers.prepare import prepare_partition
from pipelines.rj_smas__cadunico.helpers.report import format_count, format_duration, format_size
from pipelines.rj_smas__cadunico.types import Partition, RawFolders
from utils.tasks import upload_files_to_gcs
from utils.types import Cloud, Table


async def ingest_archive(name: str, raw: RawFolders, destination: Table, cloud: Cloud, label: str) -> Partition:
    """Download a ZIP, turn it into Parquet files and load them as one day of staging.

    The load is an `append` whose capture is the date of the ZIP, so a rerun adds
    nothing twice. `label` names the ZIP in the logs. Return the partition, whose
    folder is gone by then.
    """
    started = monotonic()
    logger.info("⏳ %s: download, extração e conversão para Parquet", label)

    with TemporaryDirectory(prefix=WORK_FOLDER) as directory:
        partition = await to_thread(prepare_partition, raw["bucket"], name, Path(directory))
        logger.info(
            "   %s: Parquet pronto em %s | zip=%s | txt=%d | linhas=%s | parquet=%d",
            label,
            format_duration(monotonic() - started),
            format_size(partition.archive_bytes),
            partition.texts,
            format_count(partition.rows),
            partition.files,
        )

        if partition.date != partition.extracted:
            logger.warning(
                "⚠️ %s: a data do nome é %s, e a data dentro do arquivo é %s",
                label,
                partition.date,
                partition.extracted,
            )

        uploading = monotonic()
        await upload_files_to_gcs.fn(
            partition.folder,
            destination,
            bucket=cloud["bucket"],
            dump_mode="append",
            source_format="parquet",
            options={"capture": datetime.fromisoformat(partition.date)},
        )

    logger.info(
        "✅ %s: carregado em %s (envio %s) | data=%s | layout=%s",
        label,
        format_duration(monotonic() - started),
        format_duration(monotonic() - uploading),
        partition.date,
        partition.layout_version,
    )

    return partition
