"""Tasks of rj_smas__cadunico."""

from asyncio import Semaphore, gather, to_thread
from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic

from prefect import task
from toolkit.dbt import DbtRun
from toolkit.telemetry import logger

from pipelines.rj_smas__cadunico.constants import (
    MAX_CONCURRENT,
    MODELS_FOLDER,
    SKIPPED_TABLE,
    VERSION_CONTROL_SUFFIX,
    WORK_FOLDER,
)
from pipelines.rj_smas__cadunico.helpers.ingest import ingest_archive
from pipelines.rj_smas__cadunico.helpers.layout import layout_for_versions, unmapped_message
from pipelines.rj_smas__cadunico.helpers.models import render_model_files
from pipelines.rj_smas__cadunico.helpers.prepare import read_layouts, to_records, write_table
from pipelines.rj_smas__cadunico.helpers.report import (
    compare_keys,
    format_count,
    format_duration,
    format_size,
    number_lines,
    short_name,
    summarize_models,
)
from pipelines.rj_smas__cadunico.helpers.repository import push_models
from pipelines.rj_smas__cadunico.helpers.storage import (
    list_layout_files,
    list_record_archives,
    read_staging_dates,
    read_staging_versions,
)
from pipelines.rj_smas__cadunico.helpers.versions import control_versions
from pipelines.rj_smas__cadunico.types import Partition, RawFolders, Record, Tables
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
    logger.info("🔍 Registros: lendo gs://%s/%s e o staging", raw["bucket"], raw["records_prefix"])
    staged = await to_thread(read_staging_dates, cloud["bucket"], tables["dataset_id"], tables["records"])
    archives = await to_thread(list_record_archives, raw["bucket"], raw["records_prefix"])
    new = [name for date, name in sorted(archives.items()) if date not in staged]

    logger.info("📊 Registros por data (raw vs staging):\n%s", compare_keys(archives, staged))
    logger.info(
        "📈 Registros: %d ZIPs no raw | %d datas no staging | %d para carregar",
        len(archives),
        len(staged),
        len(new),
    )

    if not new:
        logger.info("✅ Registros: o staging está em dia, nada para carregar")

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
    started = monotonic()
    logger.info(
        "🚀 Ingestão: %d ZIP(s), %d por vez:\n%s",
        len(names),
        MAX_CONCURRENT,
        number_lines([short_name(name) for name in names]),
    )

    async def ingest(index: int, name: str) -> Partition:
        """Load a ZIP, `MAX_CONCURRENT` at a time, and log its failure."""
        label = f"[{index}/{len(names)}] {short_name(name)}"

        async with semaphore:
            try:
                return await ingest_archive(name, raw, destination, cloud, label)
            except Exception as error:
                logger.error("❌ %s: falhou: %s: %s", label, type(error).__name__, error)
                raise

    results = await gather(*(ingest(index, name) for index, name in enumerate(names, 1)), return_exceptions=True)
    loaded = [result for result in results if isinstance(result, Partition)]
    failed = {name: result for name, result in zip(names, results, strict=True) if isinstance(result, BaseException)}

    if loaded:
        await publish_table_in_bq.fn(destination, cloud)

    logger.info(
        "📊 Ingestão concluída em %s: %d/%d ZIPs | %s | %s linhas | datas: %s",
        format_duration(monotonic() - started),
        len(loaded),
        len(names),
        format_size(sum(partition.archive_bytes for partition in loaded)),
        format_count(sum(partition.rows for partition in loaded)),
        ", ".join(sorted(partition.date for partition in loaded)) or "nenhuma",
    )

    if failed:
        logger.error(
            "❌ %d ZIP(s) com falha:\n%s",
            len(failed),
            number_lines([f"{short_name(name)}: {type(error).__name__}: {error}" for name, error in failed.items()]),
        )
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
    new = sorted(version for version in files if version not in staged)

    logger.info("📊 Layouts por versão (raw vs staging):\n%s", compare_keys(files, staged))
    logger.info(
        "📈 Layouts: %d no raw | %d no staging | novos: %s", len(files), len(staged), ", ".join(new) or "nenhum"
    )

    if not new:
        logger.info("✅ Layouts: nenhuma versão nova, a tabela %s fica como está", tables["layout"])
        return False

    started = monotonic()

    with TemporaryDirectory(prefix=WORK_FOLDER) as directory:
        folder = Path(directory)
        rows = await to_thread(read_layouts, raw["bucket"], files, folder)
        await load_folder(await to_thread(write_table, rows, folder), tables, tables["layout"], cloud, overwrite=True)

    logger.info(
        "✅ Layouts carregados em %s: %d planilhas | %s linhas na tabela %s",
        format_duration(monotonic() - started),
        len(files),
        format_count(len(rows)),
        tables["layout"],
    )

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
        logger.error("❌ Layout: %d coluna(s) sem linha no dicionário", unmapped.height)
        raise ValueError(unmapped_message(to_records(unmapped)))

    layout = await read_bq.fn(render_query(__file__, "layout", skipped=SKIPPED_TABLE, **names), project=project)
    versions = await to_thread(
        read_staging_versions,
        project,
        staging_name(cloud, tables, tables["records"]),
        render_query(__file__, "versions", table=staging_name(cloud, tables, tables["records"])),
    )
    rows = layout_for_versions(to_records(layout), versions)

    logger.info(
        "📋 Layout: %s linhas para as %d versões dos registros: %s",
        format_count(len(rows)),
        len(versions),
        ", ".join(sorted(versions)),
    )

    return rows


@task
async def control_versions_task(rows: list[Record], tables: Tables, cloud: Cloud) -> list[Record]:
    """Add the control of the columns between versions to the layout.

    It loads the result to a table of its own.
    """
    controlled, warnings = control_versions(rows)
    table_id = f"{tables['layout']}{VERSION_CONTROL_SUFFIX}"

    if warnings:
        logger.warning("⚠️ Inconsistências entre versões: %d aviso(s)\n%s", len(warnings), number_lines(warnings))
    else:
        logger.info("✅ Inconsistências entre versões: nenhuma, as colunas batem entre as versões")

    with TemporaryDirectory(prefix=WORK_FOLDER) as directory:
        folder = await to_thread(write_table, controlled, Path(directory))
        await load_folder(folder, tables, table_id, cloud, overwrite=True)

    logger.info("✅ Controle de versões: %s linhas na tabela %s", format_count(len(controlled)), table_id)

    return controlled


@task
async def push_models_task(rows: list[Record], tables: Tables, cloud: Cloud, dbt: DbtRun) -> bool:
    """Write the dbt models of the records, one for each record type, and push them.

    They go to the `dbt` branch.

    Return whether the push changed the repository.
    """
    files = render_model_files(rows, __file__, tables["dataset_id"], tables["records"], cloud["project"])
    logger.info(
        "✅ Modelos dbt: %d modelo(s), %d arquivo(s) em %s:\n%s",
        len(files) // 2,
        len(files),
        MODELS_FOLDER,
        summarize_models(rows),
    )

    commit = await to_thread(
        push_models, dbt.repository, dbt.branch, files, f"feat: update CadUnico models - {tables['dataset_id']}"
    )

    if commit:
        logger.info("✅ Push: commit %s na branch %s", commit[:7], dbt.branch)
    else:
        logger.info("✅ Push: os modelos da branch %s já estavam em dia, nada para commitar", dbt.branch)

    return commit is not None
