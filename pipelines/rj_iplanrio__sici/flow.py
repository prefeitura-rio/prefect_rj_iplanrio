"""Flow for rj_iplanrio__sici."""

from pathlib import Path
from tempfile import TemporaryDirectory

from prefect import flow

from pipelines.rj_iplanrio__sici.tasks import fetch_units_task
from utils.hooks import notify_failure
from utils.tasks import publish_table_in_bq, upload_files_to_gcs, write_records
from utils.types import Cloud, DumpMode, Table
from utils.utils import capture_options


@flow(log_prints=True, flow_run_name="{table[table_id]}", on_failure=[notify_failure])
async def rj_iplanrio__sici(*, cloud: Cloud, table: Table, wsdl: str, dump_mode: DumpMode = "overwrite") -> None:
    """Load the administrative units of the SICI SOAP API to a BigQuery table."""
    units = fetch_units_task(wsdl)

    with TemporaryDirectory() as directory:
        folder = await write_records(units, Path(directory))
        await upload_files_to_gcs(
            folder,
            table,
            bucket=cloud["bucket"],
            dump_mode=dump_mode,
            source_format="json",
            options=capture_options(dump_mode),
        )

    await publish_table_in_bq(table, cloud)
