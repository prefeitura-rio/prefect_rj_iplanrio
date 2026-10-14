"""Prefect flows that more than one pipeline uses, built on the shared tasks."""

from prefect import flow
from toolkit.dbt import DbtRun

from .constants import RETRIES, RETRY_DELAY_SECONDS
from .hooks import notify_failure
from .tasks import dump_db_to_gcs, publish_table_in_bq, run_dbt
from .types import Cloud, Database, DumpMode, DumpOptions, Read, Table


@flow(
    flow_run_name="{table[table_id]}",
    log_prints=True,
    on_failure=[notify_failure],
)
async def dump_database(
    *,
    database: Database,
    cloud: Cloud,
    table: Table,
    read: Read,
    dump_mode: DumpMode = "append",
    options: DumpOptions | None = None,
) -> None:
    """Dump a table or a native query of a database to a BigQuery table, without repeats.

    DuckDB reads the source one window at a time and writes a Delta table in the bucket.
    A window that did not change is never written again, so a rerun, or a retry after a
    failure, repeats nothing. BigQuery reads the Delta table through a BigLake
    connection. Set `database` and `cloud` in the `prefect.yaml` of the pipeline, and
    `table`, `read` and `options` in each schedule. A pipeline imports this flow in its
    `flow.py` and names it with `with_options(name=...)`. A failed run posts to Discord
    if `DISCORD_WEBHOOK_URL` is set.
    """
    await dump_db_to_gcs.with_options(retries=RETRIES, retry_delay_seconds=RETRY_DELAY_SECONDS)(
        database,
        table,
        read,
        bucket=cloud["bucket"],
        dump_mode=dump_mode,
        options=options,
    )
    await publish_table_in_bq.with_options(retries=RETRIES, retry_delay_seconds=RETRY_DELAY_SECONDS)(table, cloud)


@flow(
    flow_run_name="dbt-{request.command}-{request.target}",
    log_prints=True,
    on_failure=[notify_failure],
)
async def dbt_run(*, request: DbtRun) -> None:
    """Run one dbt command from a queries repository.

    The repository is cloned, `dbt deps` runs, and then the command of `request`. Set
    `request` in the `prefect.yaml` of the pipeline, with the repository, the command,
    the target and the selection. A failed run posts to Discord if `DISCORD_WEBHOOK_URL`
    is set.
    """
    await run_dbt(request)
