"""Flow for rj_iplanrio__sici."""

from prefect import flow

from pipelines.rj_iplanrio__sici.tasks import fetch_units_task
from utils.flows import load_records
from utils.hooks import notify_failure
from utils.types import Cloud, DumpMode, Table


@flow(log_prints=True, flow_run_name="{table[table_id]}", on_failure=[notify_failure])
async def rj_iplanrio__sici(*, cloud: Cloud, table: Table, wsdl: str, dump_mode: DumpMode = "overwrite") -> None:
    """Load the administrative units of the SICI SOAP API to a BigQuery table."""
    units = fetch_units_task(wsdl)

    await load_records(units, table, cloud, dump_mode=dump_mode)
