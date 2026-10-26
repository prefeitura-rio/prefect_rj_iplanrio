"""Tasks of rj_smas__cadunico."""

from asyncio import Semaphore, gather, to_thread
from pathlib import Path
from tempfile import TemporaryDirectory

from prefect import task
from toolkit.dbt import DbtRun
from toolkit.telemetry import logger

from pipelines.rj_smas__cadunico.constants import (
    MAX_CONCURRENT,
    SKIPPED_TABLE,
    VERSION_CONTROL_SUFFIX,
    WORK_FOLDER,
)
from pipelines.rj_smas__cadunico.helpers.ingest import ingest_archive
from pipelines.rj_smas__cadunico.helpers.layout import layout_for_versions, unmapped_message
from pipelines.rj_smas__cadunico.helpers.models import render_model_files
from pipelines.rj_smas__cadunico.helpers.prepare import read_layouts, to_records, write_table
from pipelines.rj_smas__cadunico.helpers.repository import push_models
from pipelines.rj_smas__cadunico.helpers.storage import (
    list_layout_files,
    list_record_archives,
    read_staging_dates,
    read_staging_versions,
)
from pipelines.rj_smas__cadunico.helpers.versions import control_versions
from pipelines.rj_smas__cadunico.types import RawFolders, Record, Tables
from utils.tasks import publish_table_in_bq, read_bq, upload_files_to_gcs
from utils.types import Cloud, Table
from utils.utils import render_query


def staging_name(cloud: Cloud, tables: Tables, table_id: str) -> str:
    """Return the full name of a staging table in BigQuery."""
    return f"{cloud['project']}.{tables['dataset_id']}_staging.{table_id}"


async def load_folder(folder: Path, tables: Tables, table_id: str, cloud: Cloud, *, overwrite: bool) -> None:
    """Load the Parquet files of a folder into a table, and refresh BigQuery.

    It replaces the table, or adds one day to it.
    """
    destination = Table(dataset_id=tables["dataset_id"], table_id=table_id)
    await upload_files_to_gcs.fn(
        folder, destination, bucket=cloud["bucket"], dump_mode="overwrite" if overwrite else "append"
    )
    await publish_table_in_bq.fn(destination, cloud)


@task
async def list_new_archives_task(raw: RawFolders, tables: Tables, cloud: Cloud) -> list[str]:
    """Return the record ZIPs in the raw bucket that staging lacks, oldest first."""
    staged = await to_thread(read_staging_dates, cloud["bucket"], tables["dataset_id"], tables["records"])
    archives = await to_thread(list_record_archives, raw["bucket"], raw["records_prefix"])
    new = [name for date, name in sorted(archives.items()) if date not in staged]
    logger.info("Records: %d ZIPs in raw, %d dates in staging, %d to load", len(archives), len(staged), len(new))

    return new


@task
async def ingest_archives_task(names: list[str], raw: RawFolders, tables: Tables, cloud: Cloud) -> None:
    """Load each ZIP of records as one day of staging, `MAX_CONCURRENT` at a time.

    The load is an `append` whose capture is the date of the ZIP, so a rerun adds
    nothing twice. A ZIP that fails does not stop the others, and the task raises
    `RuntimeError` for them at the end.
    """
    semaphore = Semaphore(MAX_CONCURRENT)
    destination = Table(dataset_id=tables["dataset_id"], table_id=tables["records"])

    async def ingest(name: str) -> None:
        """Load a ZIP, `MAX_CONCURRENT` at a time."""
        async with semaphore:
            await ingest_archive(name, raw, destination, cloud)

    results = await gather(*(ingest(name) for name in names), return_exceptions=True)
    failed = {name: result for name, result in zip(names, results, strict=True) if isinstance(result, BaseException)}

    if len(failed) < len(names):
        await publish_table_in_bq.fn(destination, cloud)

    for name, error in failed.items():
        logger.error("Failed %s: %s", name, error)

    if failed:
        raise RuntimeError(f"{len(failed)} of {len(names)} ZIPs failed: {', '.join(failed)}")


@task
async def sync_layouts_task(raw: RawFolders, tables: Tables, cloud: Cloud) -> bool:
    """Load the layout spreadsheets if the raw bucket has a version the table lacks.

    A new version replaces the table, so every spreadsheet is read again. Return whether
    the table changed.
    """
    staged = await to_thread(
        read_staging_versions,
        cloud["project"],
        staging_name(cloud, tables, tables["layout"]),
        render_query(__file__, "versions", table=staging_name(cloud, tables, tables["layout"])),
    )
    files = await to_thread(list_layout_files, raw["bucket"], raw["layout_prefix"])
    logger.info("Layouts: %d in raw %s, %d in staging %s", len(files), sorted(files), len(staged), sorted(staged))

    if all(version in staged for version in files):
        return False

    with TemporaryDirectory(prefix=WORK_FOLDER) as directory:
        folder = Path(directory)
        rows = await to_thread(read_layouts, raw["bucket"], files, folder)
        await load_folder(await to_thread(write_table, rows, folder), tables, tables["layout"], cloud, overwrite=True)

    return True


@task
async def read_layout_task(tables: Tables, cloud: Cloud) -> list[Record]:
    """Return the layout of the record versions, with the dictionary of its columns.

    Raise `ValueError` if a column of the layout has no row in the sheet of the
    dictionary, or if a version of the records has no layout.
    """
    project = cloud["project"]
    names = {"project": project, "dataset": tables["dataset_id"], "layout": tables["layout"]}
    unmapped = await read_bq.fn(render_query(__file__, "unmapped_columns", **names), project=project)

    if not unmapped.is_empty():
        raise ValueError(unmapped_message(to_records(unmapped)))

    layout = await read_bq.fn(render_query(__file__, "layout", skipped=SKIPPED_TABLE, **names), project=project)
    versions = await to_thread(
        read_staging_versions,
        project,
        staging_name(cloud, tables, tables["records"]),
        render_query(__file__, "versions", table=staging_name(cloud, tables, tables["records"])),
    )
    rows = layout_for_versions(to_records(layout), versions)

    logger.info("Layout: %d rows, filtered by the versions %s of the records", len(rows), sorted(versions))

    return rows


@task
async def control_versions_task(rows: list[Record], tables: Tables, cloud: Cloud) -> list[Record]:
    """Add the control of the columns between versions to the layout.

    It loads the result to a table of its own.
    """
    controlled, warnings = control_versions(rows)

    for warning in warnings:
        logger.warning(warning)

    with TemporaryDirectory(prefix=WORK_FOLDER) as directory:
        folder = await to_thread(write_table, controlled, Path(directory))
        await load_folder(folder, tables, f"{tables['layout']}{VERSION_CONTROL_SUFFIX}", cloud, overwrite=True)

    return controlled


@task
async def push_models_task(rows: list[Record], tables: Tables, cloud: Cloud, dbt: DbtRun) -> bool:
    """Write the dbt models of the records, one for each record type, and push them.

    They go to the `dbt` branch.

    Return whether the push changed the repository.
    """
    files = render_model_files(rows, __file__, tables["dataset_id"], tables["records"], cloud["project"])

    changed = await to_thread(
        push_models, dbt.repository, dbt.branch, files, f"feat: update CadUnico models - {tables['dataset_id']}"
    )
    logger.info("Models: %d files, repository changed=%s", len(files), changed)

    return changed
