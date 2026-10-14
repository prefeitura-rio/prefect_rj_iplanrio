"""Prefect flows that more than one pipeline uses, built on the shared tasks."""

from prefect import flow

from .constants import RETRIES, RETRY_DELAY_SECONDS
from .tasks import dump_to_bucket, publish_table
from .types import DumpMode
from .utils import retries_for


@flow(flow_run_name="{table_id}", log_prints=True)
async def dump_database(
    *,
    db_type: str,
    host: str,
    port: int,
    database: str,
    secret_path: str,
    query: str,
    dataset_id: str,
    table_id: str,
    bucket: str,
    project: str,
    dump_mode: DumpMode = "overwrite",
    biglake_connection: str | None = None,
    normalize_columns: bool = True,
    partition_by: tuple[str, ...] = (),
) -> None:
    """Dump a native query of a database to a BigQuery external table.

    DuckDB copies the rows from the database to the bucket, and BigQuery reads the
    files from there. Set the parameters of one database in the `prefect.yaml` of the
    pipeline. A pipeline imports this flow in its `flow.py` and names it with
    `with_options(name=...)`.
    """
    await dump_to_bucket.with_options(
        retries=retries_for(dump_mode),
        retry_delay_seconds=RETRY_DELAY_SECONDS,
    )(
        db_type,
        query,
        dataset_id,
        table_id,
        bucket=bucket,
        host=host,
        port=port,
        database=database,
        secret_path=secret_path,
        dump_mode=dump_mode,
        normalize_columns=normalize_columns,
        partition_by=partition_by,
    )
    await publish_table.with_options(retries=RETRIES, retry_delay_seconds=RETRY_DELAY_SECONDS)(
        dataset_id,
        table_id,
        bucket=bucket,
        project=project,
        dump_mode=dump_mode,
        biglake_connection=biglake_connection,
    )
