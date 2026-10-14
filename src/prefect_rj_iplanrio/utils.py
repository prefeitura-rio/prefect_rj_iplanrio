"""Helpers of the shared tasks: where a table lives, and the steps around it."""

from asyncio import to_thread
from collections.abc import Mapping
from dataclasses import dataclass
from http import HTTPStatus
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

from deltalake import DeltaTable
from httpx2 import HTTPStatusError, TransportError
from obstore.store import ObjectStore
from psycopg.sql import Composable, Identifier, Literal
from toolkit.batches import (
    NULL_WINDOW,
    Rows,
    Window,
    batch_column,
    config_hash,
    ledger_id,
    pending,
    window_predicates,
    windows,
)
from toolkit.bigquery import BigQuery
from toolkit.constants import BATCH_COLUMN
from toolkit.delta import (
    fingerprint,
    known_fingerprints,
    maintain,
    sweep,
    write_window,
)
from toolkit.executor.constants import ALIAS, SECRET
from toolkit.executor.engines.duckdb import DuckDBEngine
from toolkit.executor.executor import Executor
from toolkit.executor.sources.protocols import Source, Stager
from toolkit.functions.native import now_sao_paulo
from toolkit.gcs import create_store, delete_keys, list_keys, upload
from toolkit.telemetry import logger
from toolkit.types import Batches, Described, TemplateValue
from toolkit.utils import filter_predicates, select_columns

from .constants import DISCORD_LIMIT, STAGING_PREFIX
from .types import DumpMode, DumpOptions, FileFormat, Read


@dataclass(frozen=True, slots=True)
class Destination:
    """Where the files of one BigQuery table live in the bucket."""

    bucket: str
    dataset_id: str
    table_id: str

    @property
    def prefix(self) -> str:
        """Return the bucket folder of the table files."""
        return f"{STAGING_PREFIX}/{self.dataset_id}/{self.table_id}"

    @property
    def uri(self) -> str:
        """Return the `gs://` URI of the table files."""
        return f"gs://{self.bucket}/{self.prefix}"

    def dataset(self, project: str) -> str:
        """Return the BigQuery dataset of the external table."""
        return f"{project}.{self.dataset_id}_staging"

    def table(self, project: str) -> str:
        """Return the BigQuery external table."""
        return f"{self.dataset(project)}.{self.table_id}"


async def clear_prefix(store: ObjectStore, destination: Destination) -> None:
    """Delete the files of the table from the bucket."""
    await delete_keys(store, await list_keys(store, destination.prefix))


async def publish(
    destination: Destination,
    project: str,
    biglake_connection: str | None,
) -> None:
    """Create the BigQuery table over the Delta table of the bucket, or refresh its schema.

    A Delta table is a BigLake table, so it needs `biglake_connection`. Raise
    `ValueError` without it.
    """
    if not biglake_connection:
        raise ValueError("A Delta table needs a BigLake connection")

    async with BigQuery.connect(project) as bigquery:
        await bigquery.create_dataset(destination.dataset(project))
        await bigquery.sync_delta_table(destination.table(project), destination.uri, biglake_connection)


def source_values(source_type: str, query: str) -> dict[str, TemplateValue]:
    """Return the template values that read `query` from a source.

    `query` is the native query, the ArcGIS layer URL, the spreadsheet id or the path
    of the files, as the type says.
    """
    return {
        "type": source_type,
        "alias": Literal(ALIAS),
        "secret": Literal(SECRET),
        "query": Literal(query),
    }


def files_glob(data_path: Path, source_format: FileFormat) -> str:
    """Return the path of a file, or the glob of the files of a folder."""
    if data_path.is_dir():
        return f"{data_path}/**/*.{source_format}"
    return str(data_path)


async def describe_columns(executor: Executor, values: Mapping[str, TemplateValue]) -> dict[str, str]:
    """Return the name and the DuckDB type of each column that a source returns."""
    described = await executor.query("duckdb/describe_source", values, expect=Described)
    return dict(
        zip(
            described.frame["column_name"],
            described.frame["column_type"],
            strict=True,
        )
    )


def is_transient(error: object) -> bool:
    """Tell if a failed request is worth sending again.

    A timeout, a network error, a 429 and a 5xx are. A 4xx is not, because the same
    request would fail the same way.
    """
    match error:
        case HTTPStatusError():
            status = error.response.status_code
            return status == HTTPStatus.TOO_MANY_REQUESTS or status >= HTTPStatus.INTERNAL_SERVER_ERROR
        case TransportError():
            return True
        case _:
            return False


def failure_message(flow_name: str, run_name: str, reason: str, url: str | None) -> str:
    """Return the Discord message of a failed flow run, short enough to send."""
    link = f"\n{url}" if url else ""
    return f":red_circle: **{flow_name}** `{run_name}` failed: {reason}{link}"[:DISCORD_LIMIT]


def config_of(values: dict[str, TemplateValue], options: DumpOptions) -> str:
    """Hash everything that changes the bytes of a window.

    That is what is read, and the filters, the columns and the normalization. The range
    of the windows and `lookback` are left out: they change which windows run, not what
    a window holds.
    """
    batches = options.get("batches")
    return config_hash(
        {
            "read": {
                name: value.as_string(None) if isinstance(value, Composable) else value
                for name, value in values.items()
            },
            "filters": options.get("filters"),
            "select": options.get("select"),
            "columns": options.get("columns"),
            "batches": batches and {"column": batches["column"], "width": batches["width"]},
        }
    )


def check_batch_column(batches: Batches, described: Mapping[str, str]) -> None:
    """Raise `ValueError` unless the type of the batch column fits the width of the windows.

    A date width needs a date or a timestamp, and a number of ids needs an integer. A
    text column holding dates would fail later, when the database compares it to a date.
    """
    column, kind = batches["column"], described[batches["column"]]
    match batches["width"]:
        case int():
            fits, wanted = "INT" in kind, "an integer"
        case _:
            fits, wanted = kind.startswith(("DATE", "TIMESTAMP")), "a date or a timestamp"

    if not fits:
        raise ValueError(f"The batch column {column!r} is {kind}, and these windows need {wanted} column")


async def dump_to_delta(
    source: Source,
    values: dict[str, TemplateValue],
    uri: str,
    dump_mode: DumpMode,
    options: DumpOptions,
) -> None:
    """Dump what a source returns into a Delta table, one window at a time.

    For each window DuckDB extracts the rows (with the filters, the columns and the
    normalization) to a local Parquet file and fingerprints it. `append` skips a window
    whose fingerprint is in the ledger of the table, and replaces one that changed.
    Windows that were never loaded under the same config are always read, and `lookback`
    limits the re-checks of the others. `overwrite` writes every window again, then
    deletes the windows outside the config. No `batches` means one window for the whole
    source. With `batches`, the rows whose column is NULL go in a last window, `null`, that
    every run reads.

    `values` are those of `source_values`, plus the ones that the source type needs.
    """
    batches = options.get("batches")
    columns = options.get("columns", {})

    config = config_of(values, options)
    cut = [*windows(batches, now_sao_paulo().date()), NULL_WINDOW] if batches else [Window("all", 0, 0)]
    ids = {window.key: ledger_id(config, window.key) for window in cut}
    known = await to_thread(known_fingerprints, uri, list(ids.values()))
    seen = {key for key, name in ids.items() if name in known}
    todo = cut if dump_mode == "overwrite" else pending(cut, seen, (batches or {}).get("lookback"))
    written = skipped = 0

    stager = source if isinstance(source, Stager) else None
    filters = options.get("filters", [])

    async with DuckDBEngine.connect() as engine, source.open(engine) as client:
        await source.setup(client)
        executor = Executor(engine)
        with TemporaryDirectory() as directory:
            read = await stager.stage(client, values, Rows(empty=True), Path(directory)) if stager else values
            described = await describe_columns(executor, read)
        if BATCH_COLUMN in described or (batches and batches["column"] not in described):
            raise ValueError(f"The column {BATCH_COLUMN!r} is reserved, and the batch column must exist")
        if batches:
            check_batch_column(batches, described)

        base = select_columns(
            described,
            normalize_names=columns.get("normalize_columns", True),
            normalize_values=columns.get("normalize_values", False),
            transforms=columns.get("transforms"),
            select=options.get("select"),
        )
        where = filter_predicates(filters, described)

        for window in todo:
            with TemporaryDirectory() as directory:
                folder = Path(directory)
                rows = Rows(filters, batches["column"] if batches else None, window if batches else None)
                read = await stager.stage(client, values, rows, folder) if stager else values
                window_filter = window_predicates(batches["column"], window) if batches else []
                await executor.execute(
                    "duckdb/extract_window",
                    {
                        **read,
                        "columns": [*base, batch_column(window.key)],
                        "predicates": [] if stager else [*where, *window_filter],
                        "target": Literal(str(folder / "out.parquet")),
                    },
                )
                number = await fingerprint(executor, folder)

                if dump_mode == "append" and known.get(ids[window.key]) == number:
                    skipped += 1
                    continue

                await to_thread(
                    write_window,
                    uri,
                    folder,
                    window.key,
                    ids[window.key],
                    number,
                    overwrite_schema=dump_mode == "overwrite",
                )
                written += 1

    if dump_mode == "overwrite":
        await to_thread(sweep, uri, [window.key for window in cut])

    if written:
        await to_thread(maintain, uri)

    logger.info("Delta dump finished: uri=%s windows=%d written=%d skipped=%d", uri, len(cut), written, skipped)


def read_values(db_type: str, read: Read, batched: bool) -> dict[str, TemplateValue]:
    """Return the template values that read a table or a native query of a database.

    A table is attached as `src`, so DuckDB pushes the filters and the windows down to
    the database. A native query runs as it is, so it cannot be cut into windows. Oracle
    is the exception: it applies the filters and the windows itself, on a table or on a
    query. Raise `ValueError` unless `read` has exactly one of `table` and `query`, or for
    windows over a query.
    """
    match read.get("table"), read.get("query"):
        case str() as table, None:
            if db_type == "oracle":
                return {"type": db_type, "table": table}
            return {
                "type": db_type,
                "alias": Literal(ALIAS),
                "secret": Literal(SECRET),
                "table": Identifier(ALIAS, *table.split(".")),
            }
        case None, str() as query:
            if db_type == "oracle":
                return {"type": db_type, "query": query}
            if batched:
                raise ValueError("A native query is one window: set a table to cut it")
            return source_values(db_type, query)
        case _:
            raise ValueError("Give exactly one of table and query")


async def clear_legacy(destination: Destination, dump_mode: DumpMode) -> None:
    """Delete the files of an older layout before an `overwrite` builds a Delta table.

    A Delta table is never deleted: `overwrite` rewrites its windows instead.
    """
    if dump_mode == "overwrite" and not await to_thread(DeltaTable.is_deltatable, destination.uri):
        await clear_prefix(create_store(destination.bucket), destination)


async def load_to_delta(
    source: Source,
    values: dict[str, TemplateValue],
    destination: Destination,
    dump_mode: DumpMode,
    options: DumpOptions | None,
) -> None:
    """Dump a source into the Delta table of a destination.

    An `overwrite` first deletes the files of an older layout. See `dump_to_delta`.
    """
    await clear_legacy(destination, dump_mode)
    await dump_to_delta(source, values, destination.uri, dump_mode, options or {})


def without_batches(options: DumpOptions | None, source: str) -> DumpOptions:
    """Return the options of a source that DuckDB cannot filter, which are not cut in windows.

    Windows over such a source would read it whole once for each window. Raise
    `ValueError` if `options` has `batches`.
    """
    chosen = options or {}
    if "batches" in chosen:
        raise ValueError(f"{source} is read whole, so it is one window: drop batches")
    return chosen


async def dump_in_batches(
    source: Source,
    values: dict[str, TemplateValue],
    destination: Destination,
    dump_mode: DumpMode,
    batch_size: int,
) -> None:
    """Copy the rows of a source to Parquet files in the bucket, `batch_size` rows at a time.

    It reads with the `query` of the source and uploads each batch, so memory holds one
    batch. This is plain batching: a rerun in `append` adds the rows again. `overwrite`
    deletes the old files first.
    """
    store = create_store(destination.bucket)

    if dump_mode == "overwrite":
        await clear_prefix(store, destination)

    async with DuckDBEngine.connect() as engine, source.open(engine) as client:
        await source.setup(client)

        async for dataset in Executor(client).query("duckdb/select_source", values, batch_size=batch_size):
            with TemporaryDirectory() as directory:
                path = Path(directory) / f"part-{uuid4().hex}.parquet"
                await to_thread(dataset.frame.write_parquet, path)
                await upload(store, destination.prefix, path)
