"""Flow for rj_cor__precipitacao_alertario_sftp."""

from pathlib import Path
from tempfile import TemporaryDirectory

from prefect import flow
from toolkit.telemetry import logger

from pipelines.rj_cor__precipitacao_alertario_sftp.constants import METEOROLOGICAL_TYPES, PLUVIOMETRIC_TYPES
from pipelines.rj_cor__precipitacao_alertario_sftp.settings import Settings
from pipelines.rj_cor__precipitacao_alertario_sftp.tasks import build_rows_task, fetch_files_task, read_watermark_task
from utils.constants import DEFAULT_CLOUD
from utils.hooks import notify_failure
from utils.tasks import publish_table_in_bq, upload_files_to_gcs, write_parquet_records
from utils.types import Cloud, DumpMode, Table
from utils.utils import capture_options


@flow(log_prints=True, flow_run_name="precipitacao-alertario-sftp", on_failure=[notify_failure])
async def rj_cor__precipitacao_alertario_sftp(
    *,
    cloud: Cloud = DEFAULT_CLOUD,
    pluviometric: Table,
    meteorological: Table | None = None,
    dump_mode: DumpMode = "append",
    max_date_bigquery: str | None = None,
) -> None:
    """Load the AlertaRio readings that the SFTP leaves as XML in a bucket to BigQuery.

    Only the files after the latest `data_medicao` of the pluviometric table are read,
    or after `max_date_bigquery` (`YYYY-MM-DD HH:MM:SS`) when it is given. The
    meteorological table is optional.
    """
    settings = Settings()
    watermark = max_date_bigquery or await read_watermark_task(
        f"{cloud['project']}.{pluviometric['dataset_id']}_staging.{pluviometric['table_id']}", cloud["project"]
    )
    xml_files = await fetch_files_task(settings.NIMBUS_BUCKET_NAME, settings.NIMBUS_PREFIX, watermark)

    if not xml_files:
        logger.warning("No new XML file: watermark=%s", watermark)
        return

    pluviometric_rows, meteorological_rows = build_rows_task(xml_files)

    for table, rows, types in (
        (pluviometric, pluviometric_rows, PLUVIOMETRIC_TYPES),
        (meteorological, meteorological_rows, METEOROLOGICAL_TYPES),
    ):
        if table is None or not rows:
            continue

        with TemporaryDirectory() as directory:
            folder = await write_parquet_records(rows, Path(directory), types)
            await upload_files_to_gcs(
                folder,
                table,
                bucket=cloud["bucket"],
                dump_mode=dump_mode,
                source_format="parquet",
                options=capture_options(dump_mode),
            )

        await publish_table_in_bq(table, cloud)
