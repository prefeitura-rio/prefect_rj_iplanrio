"""Flow for rj_smas__cadunico."""

from prefect import flow
from toolkit.dbt import DbtRun

from pipelines.rj_smas__cadunico.constants import DEFAULT_RAW, DEFAULT_TABLES
from pipelines.rj_smas__cadunico.tasks import (
    control_versions_task,
    ingest_archives_task,
    list_new_archives_task,
    push_models_task,
    read_layout_task,
    sync_layouts_task,
)
from pipelines.rj_smas__cadunico.types import RawFolders, Tables
from utils.constants import DEFAULT_CLOUD
from utils.hooks import notify_failure
from utils.tasks import run_dbt
from utils.types import Cloud


@flow(log_prints=True, flow_run_name="cadunico-{tables[dataset_id]}", on_failure=[notify_failure])
async def rj_smas__cadunico(
    *,
    cloud: Cloud = DEFAULT_CLOUD,
    dbt: DbtRun,
    raw: RawFolders = DEFAULT_RAW,
    tables: Tables = DEFAULT_TABLES,
    force_create_models: bool = False,
) -> None:
    """Load the records of CadÚnico, and build their dbt models and tables.

    1. Each ZIP in `raw` whose date is not in the staging table becomes one day of it.
    2. With a new ZIP, or with `force_create_models`, the layout spreadsheets load to
       the layout table if the bucket has a new version.
    3. With a new layout, or with `force_create_models`, the flow reads the layout with
       the dictionary of its columns, writes one dbt model for each type of record and
       pushes the models to the branch of `dbt`.
    4. The flow runs the dbt command of `dbt` on that branch. A run with no new ZIP and
       no `force_create_models` ends after step 1.

    `dbt` is a `DbtRun`: the repository of the queries, the branch that has the models,
    the target and the selection of the models.
    """
    new = await list_new_archives_task(raw, tables, cloud)

    if new:
        await ingest_archives_task(new, raw, tables, cloud)

    if not (new or force_create_models):
        return

    if await sync_layouts_task(raw, tables, cloud) or force_create_models:
        layout = await read_layout_task(tables, cloud)
        controlled = await control_versions_task(layout, tables, cloud)
        await push_models_task(controlled, tables, cloud, dbt)

    await run_dbt(dbt)
