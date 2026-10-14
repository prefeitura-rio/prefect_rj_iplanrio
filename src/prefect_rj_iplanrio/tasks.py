"""Prefect tasks that more than one pipeline uses, built on the toolkit."""

import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory

from prefect import task
from psycopg.sql import Identifier, Literal
from toolkit import gcs
from toolkit.dbt import DbtRun, execute_dbt
from toolkit.executor.constants import ALIAS, SECRET
from toolkit.executor.engines.duckdb import DuckDBEngine
from toolkit.executor.executor import Executor
from toolkit.executor.sources.protocols import DBConn, SourceContext
from toolkit.executor.sources.registry import get_source
from toolkit.settings import read_database_credentials
from toolkit.types import Described, TemplateValue
from toolkit.utils import column_aliases

from .types import DumpMode, FileFormat
from .utils import Destination, clear_prefix, csv_to_parquet, publish


@task
async def load_to_bigquery(
    data_path: Path,
    dataset_id: str,
    table_id: str,
    *,
    bucket: str,
    project: str,
    dump_mode: DumpMode = "overwrite",
    source_format: FileFormat = "parquet",
    biglake_connection: str | None = None,
) -> None:
    """Upload local files to the bucket and publish them as a BigQuery table.

    `overwrite` deletes the old files first. `append` keeps them, and a file with the
    same name replaces the old one. Prefer Parquet: `csv` is for data that a pipeline
    cannot produce as Parquet.
    """
    destination = Destination(bucket, dataset_id, table_id)
    store = gcs.create_store(bucket)

    with TemporaryDirectory() as directory:
        path = await csv_to_parquet(data_path, Path(directory)) if source_format == "csv" else data_path
        if dump_mode == "overwrite":
            await clear_prefix(store, destination)
        await gcs.upload(store, destination.prefix, path)

    await publish(destination, project, dump_mode, biglake_connection)


@task
async def publish_table(
    dataset_id: str,
    table_id: str,
    *,
    bucket: str,
    project: str,
    dump_mode: DumpMode = "overwrite",
    biglake_connection: str | None = None,
) -> None:
    """Create the BigQuery external table over the files that are in the bucket."""
    await publish(
        Destination(bucket, dataset_id, table_id),
        project,
        dump_mode,
        biglake_connection,
    )


@task
async def dump_to_bucket(
    db_type: str,
    query: str,
    dataset_id: str,
    table_id: str,
    *,
    bucket: str,
    host: str,
    port: int,
    database: str,
    secret_path: str,
    dump_mode: DumpMode = "overwrite",
    normalize_columns: bool = True,
    partition_by: tuple[str, ...] = (),
) -> None:
    """Copy a native query of a database to Parquet files in the bucket.

    DuckDB writes the files itself, so no row reaches Python. `secret_path` names the
    secret with the user and the password, such as `/db-oracle-x`. With
    `normalize_columns`, each column name becomes a slug, and `partition_by` names the
    slugs that give the Hive folders.
    """
    destination = Destination(bucket, dataset_id, table_id)
    credentials = read_database_credentials(secret_path)
    conn = DBConn(host, port, database, credentials.username, credentials.password)
    source = get_source(db_type).from_context(SourceContext(conn))
    values: dict[str, TemplateValue] = {
        "type": db_type,
        "alias": Literal(ALIAS),
        "secret": Literal(SECRET),
        "query": Literal(query),
    }

    if dump_mode == "overwrite":
        await clear_prefix(gcs.create_store(bucket), destination)

    async with DuckDBEngine.connect() as engine, source.open(engine) as client:
        await source.setup(client)
        executor = Executor(client)
        columns: list[dict[str, Identifier]] | bool = False

        if normalize_columns:
            described = await executor.query("duckdb/describe_native", values, expect=Described)
            names = described.frame["column_name"].to_list()
            columns = column_aliases(names)

        await executor.execute(
            "duckdb/dump_to_bucket",
            {
                **values,
                "target": Literal(destination.uri),
                "columns": columns,
                "partition_by": [Identifier(name) for name in partition_by],
            },
        )


@task
async def run_dbt(request: DbtRun) -> None:
    """Run one dbt command from a cloned queries repository."""
    await asyncio.to_thread(execute_dbt, request)
