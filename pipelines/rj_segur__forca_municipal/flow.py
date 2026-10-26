"""Flow for rj_segur__forca_municipal."""

from prefect import flow

from pipelines.rj_segur__forca_municipal.constants import TABLE_CONFIG
from pipelines.rj_segur__forca_municipal.tasks import run_qmd_task, run_standard_endpoint_task, run_unit_positions_task
from pipelines.rj_segur__forca_municipal.types import Limits, Window
from utils.constants import DEFAULT_CLOUD
from utils.hooks import notify_failure
from utils.tasks import publish_table_in_bq
from utils.types import Cloud, DumpMode, Table


@flow(log_prints=True, flow_run_name="{table[dataset_id]}.{table[table_id]}", on_failure=[notify_failure])
async def rj_segur__forca_municipal(
    *,
    cloud: Cloud = DEFAULT_CLOUD,
    table: Table,
    dump_mode: DumpMode = "append",
    limits: Limits | None = None,
    window: Window | None = None,
) -> None:
    """Read a table of the Forca Municipal API (HxGN OnCall, the Municipal Guard) and load it to BigQuery.

    `table["table_id"]` is one of the keys of `TABLE_CONFIG`: a list endpoint
    (`unidades_historico`, `qmd`, `ocorrencias_ativas_v2` and others), `unit_positions`
    (the GPS of each unit, one day at a time, from `window`), `qmd_detalhes` or
    `qmd_kml` (one request for each QMD of the latest `qmd`). `limits` sets the page
    size and how many requests, units and QMDs run at one time. Raise `ValueError` for a
    table that is not in `TABLE_CONFIG`.
    """
    table_id = table["table_id"]

    if table_id not in TABLE_CONFIG:
        raise ValueError(f"table_id {table_id!r} is not valid. Use one of {sorted(TABLE_CONFIG)}")

    if table_id == "unit_positions":
        await run_unit_positions_task(table, cloud, dump_mode, limits or {}, window or {})
    elif table_id in ("qmd_detalhes", "qmd_kml"):
        await run_qmd_task(table, cloud, dump_mode, limits or {})
    else:
        await run_standard_endpoint_task(table, cloud, dump_mode, limits or {})

    await publish_table_in_bq(table, cloud)
