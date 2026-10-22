"""Flow for rj_iplanrio__eai_history."""

from pathlib import Path
from tempfile import TemporaryDirectory

from prefect import flow
from toolkit.dbt import DbtRun
from toolkit.telemetry import logger

from pipelines.rj_iplanrio__eai_history.constants import DBT_REPOSITORY, DEFAULT_DBT_SELECT
from pipelines.rj_iplanrio__eai_history.tasks import fetch_history_task, read_last_update_task
from pipelines.rj_iplanrio__eai_history.types import Environment, Options
from utils.hooks import notify_failure
from utils.tasks import publish_table_in_bq, run_dbt, upload_files_to_gcs, write_records
from utils.types import Cloud, Table
from utils.utils import capture_options


@flow(log_prints=True, flow_run_name="eai-history-{environment}", on_failure=[notify_failure])
async def rj_iplanrio__eai_history(
    *,
    cloud: Cloud,
    table: Table,
    environment: Environment = "staging",
    options: Options | None = None,
) -> None:
    """Load the messages of the EAI agent threads that changed since the last run to BigQuery, and run dbt.

    The cursor is the latest `last_update` of the environment in `table`, or `options["last_update"]`.
    Each row holds the latest checkpoint of a thread, with its messages in the format of the gateway
    as JSON text. When the rows load, dbt runs `options["dbt_select"]` (`raw_eai_logs_history`).
    """
    options = options or {}
    last_update = options.get("last_update") or await read_last_update_task(cloud["project"], table, environment)
    rows = await fetch_history_task(last_update, environment, options)

    if not rows:
        logger.info("No thread to load")
        return

    with TemporaryDirectory() as directory:
        folder = await write_records(rows, Path(directory))
        await upload_files_to_gcs(
            folder,
            table,
            bucket=cloud["bucket"],
            dump_mode="append",
            source_format="json",
            options=capture_options("append"),
        )

    await publish_table_in_bq(table, cloud)
    await run_dbt(
        DbtRun(
            repository=DBT_REPOSITORY,
            command="run",
            target="prod",
            select=options.get("dbt_select", DEFAULT_DBT_SELECT),
        )
    )
