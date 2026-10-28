"""Flow for rj_cor__precipitacao_alertario."""

from pathlib import Path
from tempfile import TemporaryDirectory

from prefect import flow

from pipelines.rj_cor__precipitacao_alertario.constants import METEOROLOGICAL_TYPES, PLUVIOMETRIC_TYPES
from pipelines.rj_cor__precipitacao_alertario.tasks import fetch_readings_task
from utils.constants import DEFAULT_CLOUD
from utils.hooks import notify_failure
from utils.tasks import publish_table_in_bq, upload_files_to_gcs, write_parquet_records
from utils.types import Cloud, DumpMode, Table
from utils.utils import capture_options


@flow(log_prints=True, flow_run_name="{pluviometric[table_id]}", on_failure=[notify_failure])
async def rj_cor__precipitacao_alertario(
    *,
    cloud: Cloud = DEFAULT_CLOUD,
    pluviometric: Table,
    meteorological: Table | None = None,
    dump_mode: DumpMode = "append",
) -> None:
    """Add the readings of the AlertaRio stations to BigQuery tables.

    The page has the pluviometric readings (the rain over several windows) and the
    meteorological ones (temperature, humidity, pressure and wind). The meteorological
    table is optional.
    """
    pluviometric_rows, meteorological_rows = await fetch_readings_task()

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
