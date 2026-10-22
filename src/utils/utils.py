"""Helpers of the shared tasks: where a table lives, and the steps around it."""

from asyncio import to_thread
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from http import HTTPStatus
from importlib import import_module
from json import dumps
from os import environ
from pathlib import Path
from re import NOFLAG
from string import Template
from tempfile import TemporaryDirectory
from typing import cast, final
from uuid import uuid4
from zoneinfo import ZoneInfo

from deltalake import DeltaTable
from google.auth import default
from google.auth.credentials import Credentials
from httpx2 import AsyncClient, HTTPStatusError, Response, TransportError
from obstore.store import ObjectStore
from polars import DataFrame, concat
from prefect.runtime import flow_run
from pydantic import JsonValue, TypeAdapter, ValidationError
from toolkit.bigquery import BigQuery
from toolkit.chunks import (
    NULL_CHUNK,
    Chunk,
    Rows,
    build_batch_column,
    build_ledger_id,
    hash_parts,
    select_pending,
    split_chunks,
)
from toolkit.constants import ALL_KEY, BATCH_COLUMN, EXTRACTED_AT_COLUMN, TIMEZONE
from toolkit.dbt import DbtRun
from toolkit.delta import (
    compute_fingerprint,
    maintain,
    read_known_fingerprints,
    sweep,
    write_chunk,
)
from toolkit.executor.engines.base import Engine, require_origin
from toolkit.executor.engines.duckdb import DuckDBEngine
from toolkit.executor.executor import Executor
from toolkit.executor.render import render_template
from toolkit.executor.requests import Aggregate, Changes, Query, Table
from toolkit.executor.sources.backends.bigquery import BigQuerySource
from toolkit.executor.sources.protocols import Source
from toolkit.executor.types import Origin
from toolkit.functions.native import now_sao_paulo
from toolkit.gcs import create_store, delete_keys, list_keys, list_prefixes, upload
from toolkit.telemetry import logger
from toolkit.types import Columns, TemplateValue
from toolkit.utils import select_columns

from .constants import (
    CREDENTIAL_SCOPES,
    DBT_FAILED,
    DBT_WARNED,
    DISCORD_LIMIT,
    DOCUMENT_STORES,
    PIPELINES_FOLDER,
    PLACEHOLDER_NAME,
    STAGES_FOLDER,
    STAGES_NAME,
    STAGING_PREFIX,
    TOKEN_KEYS,
)
from .types import Database, DbtNode, DbtOutcome, DumpMode, DumpOptions, FileFormat, Maintenance, Read


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
        """Return the `gcs://` URI of the table files.

        `deltalake` reads `gcs://` with OpenDAL, which accepts a WIF ADC (`external_account`),
        and `gs://` with `object_store`, which does not. `BigQuery` turns it into `gs://`.
        """
        return f"gcs://{self.bucket}/{self.prefix}"

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
    connection: str | None,
) -> None:
    """Create the BigQuery table over the Delta table of the bucket, or refresh its schema.

    A Delta table is a BigLake table, so it needs `connection`. Raise
    `ValueError` without it.
    """
    if not connection:
        raise ValueError("A Delta table needs a BigLake connection")

    async with BigQuery.connect(project) as bigquery:
        await bigquery.create_dataset(destination.dataset(project))
        await bigquery.sync_delta_table(destination.table(project), destination.uri, connection)


async def discover_tables(store: ObjectStore, bucket: str) -> list[Destination]:
    """Return the Delta tables under the staging folder of a bucket.

    A folder `staging/<dataset>/<table>` with a `_delta_log` is a table.
    """
    found: list[Destination] = []

    for dataset in await list_prefixes(store, STAGING_PREFIX):
        for folder in await list_prefixes(store, dataset):
            if f"{folder}/_delta_log" in await list_prefixes(store, folder):
                found.append(Destination(bucket, dataset.rsplit("/", 1)[-1], folder.rsplit("/", 1)[-1]))

    return found


def summarize_maintenance(done: Sequence[Maintenance], failed: Mapping[str, str]) -> str:
    """Return the Discord message of a maintenance run, cut to the limit of Discord.

    It lists the failed tables first, then the tables whose layout looks wrong, then
    the files and bytes that the run removed.
    """
    lines = [f"**Delta maintenance**: {len(done)} ok, {len(failed)} failed"]
    lines += [f":red_circle: `{name}`: {reason}" for name, reason in failed.items()]
    lines += [f":warning: `{item['table']}`: {problem}" for item in done for problem in item["problems"]]

    before = sum(item["before"]["files"] for item in done)
    after = sum(item["after"]["files"] for item in done)
    lines.append(f"Files: {before} to {after}")

    return "\n".join(lines)[:DISCORD_LIMIT]


def files_glob(data_path: Path, source_format: FileFormat) -> str:
    """Return the path of a file, or the glob of the files of a folder."""
    if data_path.is_dir():
        return f"{data_path}/**/*.{source_format}"
    return str(data_path)


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


def read_nodes(result: object) -> list[DbtNode]:
    """Return the nodes of a dbt result that failed or warned, in the order of dbt.

    A command with no result per node, such as `debug`, has none.
    """
    items = cast("Sequence[object]", getattr(getattr(result, "result", None), "results", ()))
    nodes: list[DbtNode] = []

    for item in items:
        status = str(getattr(item, "status", ""))
        if status in (*DBT_FAILED, *DBT_WARNED):
            node = getattr(item, "node", item)
            name = getattr(node, "name", None) or getattr(node, "unique_id", "")
            nodes.append(DbtNode(name=str(name), status=status, message=str(getattr(item, "message", None) or "")))

    return nodes


def summarize_dbt(request: DbtRun, outcome: DbtOutcome) -> str:
    """Return the Discord message of a dbt run, cut to the limit of Discord.

    The failed and warned nodes are listed only if there are some. The repository shows
    by its name, never by its URL, because the URL can hold a token.
    """
    name = request.repository.rsplit("/", 1)[-1].removesuffix(".git")
    mark = ":white_check_mark:" if outcome["success"] else ":x:"
    ending = "without errors" if outcome["success"] else "with errors"
    lines = [f"{mark} **[{name}]** `dbt {request.command}` finished {ending}", f"- Target: `{request.target}`"]
    lines += [
        f"- {label}: `{value}`" for label, value in (("Select", request.select), ("Exclude", request.exclude)) if value
    ]

    for node in outcome["nodes"]:
        icon = ":warning:" if node["status"] in DBT_WARNED else ":red_circle:"
        lines.append(f"{icon} `{node['name']}` {node['status']}: {node['message']}")

    return "\n".join(lines)[:DISCORD_LIMIT]


def hash_config(origin: Origin, options: DumpOptions) -> str:
    """Hash everything that changes the bytes of a chunk.

    That is what is read, and the filters, the columns and the normalization. The range
    of the chunks and `lookback` are left out: they change which chunks run, not what
    a chunk holds. A capture leaves out what is read too, because its files are in a
    new folder on every run.
    """
    spec = options.get("chunks")
    return hash_parts(
        {
            "read": None if "capture" in options else {"origin": type(origin).__name__, **asdict(origin)},
            "filters": options.get("filters"),
            "select": options.get("select"),
            "columns": options.get("columns"),
            "chunks": spec and {"column": spec["column"], "width": spec["width"]},
        }
    )


def cut_chunks(options: DumpOptions, dump_mode: DumpMode) -> list[Chunk]:
    """Return the chunks of a dump: the ones of `chunks`, one for a `capture`, or `all`.

    Raise `ValueError` for a capture with `chunks` or with `overwrite`.
    """
    spec = options.get("chunks")
    capture = options.get("capture")

    if capture and (spec or dump_mode == "overwrite"):
        raise ValueError("A capture is added as one chunk: drop chunks, and do not overwrite")
    if capture:
        return [Chunk(capture.date().isoformat(), 0, 0)]
    if spec:
        return [*split_chunks(spec, now_sao_paulo().date()), NULL_CHUNK]
    return [Chunk(ALL_KEY, 0, 0)]


async def dump_to_delta(
    source: Source,
    uri: str,
    dump_mode: DumpMode,
    options: DumpOptions,
) -> None:
    """Dump what a source returns into a Delta table, one chunk at a time.

    For each chunk the engine extracts the rows (with the filters, the columns and the
    normalization) to a local Parquet file, and DuckDB fingerprints it. `append` skips a
    chunk whose fingerprint is in the ledger of the table, and replaces one that
    changed. Chunks that were never loaded under the same config are always read, and
    `lookback` limits the re-checks of the others. `overwrite` writes every chunk
    again, then deletes the chunks outside the config. No `chunks` means one chunk
    for the whole source. With `chunks`, the rows whose column is NULL go in a last
    chunk, `null`, that every run reads. The engine of the source says how many chunks
    it reads at the same time, and the chunks are written to the table one at a time. A
    `capture` is the exception: its rows are added to the table, in the chunk of its
    day, and a capture that the ledger knows is never written again. Raise `ValueError`
    for a source with no origin, for a capture with `chunks` or with `overwrite`, and
    for `filters` that are not one predicate each.

    The origin is what to read, as `read_request` or a flow builds it.
    """
    origin = require_origin(source)
    spec = options.get("chunks")
    columns = options.get("columns", {})
    filters = options.get("filters", [])

    capture = options.get("capture")
    config = hash_config(origin, options)
    cut = cut_chunks(options, dump_mode)
    ids = {chunk.key: build_ledger_id(config, capture.isoformat() if capture else chunk.key) for chunk in cut}
    known = await to_thread(read_known_fingerprints, uri, list(ids.values()))
    seen = {key for key, name in ids.items() if name in known}
    todo = cut if dump_mode == "overwrite" else select_pending(cut, seen, (spec or {}).get("lookback"))
    if capture:
        todo = [chunk for chunk in cut if chunk.key not in seen]
    extracted_at = datetime.now(UTC) if columns.get("add_timestamp") else None
    written = skipped = 0

    async with Executor(Engine.from_source(source)) as executor, DuckDBEngine() as local:
        described = await executor.describe()
        reserved = {BATCH_COLUMN, *([EXTRACTED_AT_COLUMN] if columns.get("add_timestamp") else [])}
        if reserved & set(described) or (spec and spec["column"] not in described):
            raise ValueError(f"The columns {sorted(reserved)} are reserved, and the chunk column must exist")
        if spec:
            executor.engine.check_chunks(spec, described)

        base = select_columns(
            described,
            normalize_names=columns.get("normalize_columns", True),
            normalize_values=columns.get("normalize_values", False),
            transforms=columns.get("transforms"),
            select=options.get("select"),
        )

        def plan(chunk: Chunk) -> tuple[Rows, Columns]:
            """Return the rows and the columns that one chunk reads."""
            rows = Rows(filters, spec["column"] if spec else None, chunk if spec else None)
            return rows, [*base, build_batch_column(chunk.key)]

        async def handle(chunk: Chunk, folder: Path) -> None:
            """Fingerprint a chunk and write it to the table, unless it did not change."""
            nonlocal written, skipped
            number = await compute_fingerprint(Executor(local), folder)

            if dump_mode == "append" and known.get(ids[chunk.key]) == number:
                skipped += 1
                return

            await to_thread(
                write_chunk,
                uri,
                folder,
                chunk.key,
                ids[chunk.key],
                number,
                overwrite_schema=dump_mode == "overwrite",
                append=capture is not None,
                extracted_at=extracted_at,
            )
            written += 1

        await executor.run_chunks(todo, plan, handle)

    if dump_mode == "overwrite":
        await to_thread(sweep, uri, [chunk.key for chunk in cut])

    if written:
        await to_thread(maintain, uri)

    logger.info("Delta dump finished: uri=%s chunks=%d written=%d skipped=%d", uri, len(cut), written, skipped)


def bigquery_source(project: str, origin: Origin) -> Source:
    """Return the BigQuery source of a project that reads an origin."""
    return BigQuerySource(project=project, origin=origin)


@final
class Placeholder(Template):
    """A text with `${NAME}` placeholders, where a name is `UPPER_SNAKE_CASE`."""

    idpattern = PLACEHOLDER_NAME
    flags = NOFLAG


def expand_environment(value: str) -> str:
    """Return the value with each `${NAME}` replaced by the environment variable `NAME`.

    Raise `ValueError` for a variable that is not set or is empty, and for a placeholder
    with a name that is not `UPPER_SNAKE_CASE`.
    """
    variables = {name: text for name, text in environ.items() if text}
    try:
        return Placeholder(value).substitute(variables)
    except KeyError as error:
        raise ValueError(f"The environment variable {error.args[0]} is not set") from None


def expand_database(database: Database) -> Database:
    """Return the database with the placeholders of its host and its name replaced.

    The `prefect.yaml` writes `host: ${TAXIRIO_MONGODB_HOST}`, and the environment of
    the run, with the Secret of the pipeline, gives the value.
    """
    return {
        **database,
        "host": expand_environment(database["host"]),
        "database": expand_environment(database["database"]),
    }


def locate_pipeline(name: str) -> Path:
    """Return the JSON file `<name>.json` of the aggregations of the running pipeline.

    The folder is `pipelines/<flow name>/pipelines`, and the flow has the name of its
    pipeline. Raise `ValueError` for a name that is not lowercase words with dashes.
    """
    if not STAGES_NAME.fullmatch(name):
        raise ValueError(f"The pipeline {name!r} is not lowercase words with dashes")

    return PIPELINES_FOLDER / cast("str", flow_run.flow_name) / STAGES_FOLDER / f"{name}.json"


def build_aggregate(db_type: str, read: Read) -> Aggregate:
    """Return the aggregation of a `read` that has a `pipeline`.

    The `pipeline` is the name of a JSON file with the list of stages, as
    `locate_pipeline` finds it. The text of the stages is compact, so the layout of the
    file does not change what a chunk hashes to.

    Raise `ValueError` for a database that is not MongoDB, for a read with no `table` or
    with a `query` or a `since`, and for a file that is not a JSON list of objects.
    """
    table = read.get("table")
    name = read.get("pipeline", "")

    if db_type != "mongodb":
        raise ValueError(f"{db_type} has no pipeline: only MongoDB aggregates")
    if table is None or "query" in read or "since" in read:
        raise ValueError("A pipeline needs a table, and takes no query or since")

    path = locate_pipeline(name)
    adapter = TypeAdapter(list[dict[str, JsonValue]])
    try:
        stages = adapter.validate_json(path.read_text(encoding="utf-8"))
    except ValidationError:
        raise ValueError(f"{path} is not a JSON list of stages, each one an object") from None

    return Aggregate(adapter.dump_json(stages).decode(), table)


def read_request(db_type: str, read: Read, batched: bool) -> Origin:
    """Return the origin that a `read` of a database names.

    A table is read in place, so the filters and the chunks go down to the database. A
    native query runs as it is, so it cannot be cut into chunks, except on Oracle,
    which applies the filters and the chunks itself. A document store (CouchDB and
    MongoDB) takes a `table` (the database or the collection), a `query` (a selector)
    and, for CouchDB, a `since` (a sequence of `_changes`), with no more than one of
    `query` and `since`. MongoDB also takes a `pipeline` of aggregation stages over a
    `table`. Raise `ValueError` for a read that does not fit.
    """
    table, query, since = read.get("table"), read.get("query"), read.get("since")

    if "pipeline" in read:
        return build_aggregate(db_type, read)

    if db_type in DOCUMENT_STORES:
        if query is not None and since is not None:
            raise ValueError("Give one of query and since, not both")
        if since is not None and db_type != "couchdb":
            raise ValueError(f"{db_type} has no since: only CouchDB reads changes")
        if since is not None:
            return Changes(since, table)
        if query is not None:
            return Query(query, db_type, table)
        if table is not None:
            return Table(table)
        raise ValueError("Give a table, a query or a since")

    match table, query:
        case str(), None:
            return Table(table)
        case None, str():
            if batched and db_type != "oracle":
                raise ValueError("A native query is one chunk: set a table to cut it")
            return Query(query, db_type)
        case _:
            raise ValueError("Give exactly one of table and query")


async def clear_legacy(destination: Destination, dump_mode: DumpMode) -> None:
    """Delete the files of an older layout before an `overwrite` builds a Delta table.

    A Delta table is never deleted: `overwrite` rewrites its chunks instead.
    """
    if dump_mode == "overwrite" and not await to_thread(DeltaTable.is_deltatable, destination.uri):
        await clear_prefix(create_store(destination.bucket), destination)


async def load_to_delta(
    source: Source,
    destination: Destination,
    dump_mode: DumpMode,
    options: DumpOptions | None,
) -> None:
    """Dump a source into the Delta table of a destination.

    An `overwrite` first deletes the files of an older layout. See `dump_to_delta`.
    """
    await clear_legacy(destination, dump_mode)
    await dump_to_delta(source, destination.uri, dump_mode, options or {})


def without_chunks(options: DumpOptions | None, source: str) -> DumpOptions:
    """Return the options of a source that DuckDB cannot filter, which are not cut in chunks.

    Chunks over such a source would read it whole once for each chunk. Raise
    `ValueError` if `options` has `chunks`.
    """
    chosen = options or {}
    if "chunks" in chosen:
        raise ValueError(f"{source} is read whole, so it is one chunk: drop chunks")
    return chosen


async def dump_in_batches(
    source: Source,
    destination: Destination,
    dump_mode: DumpMode,
    batch_size: int,
) -> None:
    """Copy the rows of a source to Parquet files in the bucket, `batch_size` rows at a time.

    It reads the origin of the source and uploads each batch, so memory holds one
    batch. This is plain batching: a rerun in `append` adds the rows again. `overwrite`
    deletes the old files first.
    """
    store = create_store(destination.bucket)

    if dump_mode == "overwrite":
        await clear_prefix(store, destination)

    async with Executor(Engine.from_source(source)) as executor:
        async for batch in executor.read(batch_size=batch_size):
            with TemporaryDirectory() as directory:
                path = Path(directory) / f"part-{uuid4().hex}.parquet"
                await to_thread(batch.dataframe.write_parquet, path)
                await upload(store, destination.prefix, path)


async def read_dataframe(source: Source) -> DataFrame:
    """Read every row of the origin of a source into one dataframe."""
    async with Executor(Engine.from_source(source)) as executor:
        return concat([batch.dataframe async for batch in executor.read()])


def epsg_code(crs: str) -> str:
    """Return the number of an EPSG code, such as `31983` for `EPSG:31983`.

    ```python
    epsg_code("EPSG:31983")  # "31983"
    ```

    Raise `ValueError` for anything else.
    """
    code = crs.upper().removeprefix("EPSG:")
    if not code.isdigit():
        raise ValueError(f"Not an EPSG code such as EPSG:31983: {crs!r}")

    return code


def dig(data: JsonValue, *path: str) -> JsonValue:
    """Return the value at a path of keys, or nothing if the path breaks.

    ```python
    dig({"data": {"item": {"token": "t"}}}, "data", "item", "token")  # "t"
    ```
    """
    for key in path:
        data = data.get(key) if isinstance(data, dict) else None

    return data


def read_token(data: JsonValue) -> str:
    """Return the token of a login response, in any of the places the API has put it.

    Raise `ValueError` if the response has no token.
    """
    candidates = [dig(data, key) for key in TOKEN_KEYS] + [dig(data, "data", "item", "token")]

    for candidate in candidates:
        if isinstance(candidate, str) and candidate:
            return candidate

    raise ValueError("No token in the login response")


def read_json(response: Response) -> JsonValue:
    """Return the JSON of a response with a type, so the code that reads it can check it.

    Raise `ValidationError` if the body is not JSON.
    """
    adapter: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)

    return adapter.validate_json(response.content)


def extract_records(data: JsonValue, key: str | None) -> list[dict[str, JsonValue]]:
    """Return the records of a JSON document: the list itself, or the list at a dotted `key`.

    ```python
    extract_records({"data": {"items": [{"id": 1}]}}, "data.items")  # [{"id": 1}]
    ```

    Raise `ValueError` if there is no list of objects at the key.
    """
    found = data
    for name in key.split(".") if key else []:
        found = found.get(name) if isinstance(found, dict) else None

    if not isinstance(found, list) or not all(isinstance(record, dict) for record in found):
        raise ValueError(f"No list of objects at {key or 'the top'} of the response")

    return [record for record in found if isinstance(record, dict)]


def write_json_lines(records: Sequence[Mapping[str, JsonValue]], folder: Path) -> Path:
    """Write the records as a JSON lines file in a folder, and return the folder."""
    (folder / "records.json").write_text("\n".join(dumps(record) for record in records) + "\n", encoding="utf-8")
    return folder


def load_records_schema(path: str) -> type:
    """Return the type that a path `module:name` names, to check the records of an API.

    It is a `BaseModel`, a dataclass, a `TypedDict` or any other class that Pydantic
    validates. Raise `ValueError` for a path that leads to no type.
    """
    module, _, name = path.partition(":")

    if not module or not name:
        raise ValueError(f"The records schema {path!r} is not a path of the form module:name")

    try:
        found: object = getattr(import_module(module), name, None)
    except ImportError as error:
        raise ValueError(f"The records schema {path!r} cannot be imported: {error}") from error

    if not isinstance(found, type):
        raise ValueError(f"The records schema {path!r} is not a type")

    return found


def validate_records(records: Sequence[Mapping[str, JsonValue]], schema: type) -> None:
    """Raise `ValueError` for the first record that does not fit the schema.

    The message names the index of the record and its fields, and never a value. The
    records are not changed: they are written as the API sent them.

    ```python
    validate_records([{"id": "x"}], Item)
    # ValueError: record 0: id: Input should be a valid integer
    ```
    """
    adapter = TypeAdapter(schema)

    for index, record in enumerate(records):
        try:
            adapter.validate_python(record)
        except ValidationError as error:
            problems = "; ".join(
                f"{'.'.join(str(part) for part in problem['loc']) or 'record'}: {problem['msg']}"
                for problem in error.errors()
            )
            raise ValueError(f"record {index}: {problems}") from None


def render_query(anchor: str, name: str, **values: TemplateValue | float | date | None) -> str:
    """Render `queries/<name>.sql`, next to the file `anchor`, as a strict Jinja template.

    A value that is a float, a date or `None` is written as its text. Raise
    `UndefinedError` for a variable that `values` lacks.

    ```python
    render_query(__file__, "last_update", table="p.d.t")  # SELECT max(x) FROM p.d.t
    ```
    """
    plain: dict[str, TemplateValue] = {
        key: str(value) if isinstance(value, float | date | None) else value for key, value in values.items()
    }

    return render_template(name, plain, roots=(Path(anchor).parent / "queries",))


def load_credentials(scopes: Sequence[str] = CREDENTIAL_SCOPES) -> Credentials:
    """Return the ADC credentials (workload identity) for the scopes.

    Raise `DefaultCredentialsError` if the environment has no credentials.
    """
    return cast("Credentials", default(scopes=list(scopes))[0])


def capture_options(dump_mode: DumpMode) -> DumpOptions:
    """Return the options that make an `append` run safe to send again, or none.

    An `append` of a run adds its rows under the capture time of the run, so a retry of
    the same run adds nothing. An `overwrite` replaces the table and needs none.
    """
    return DumpOptions(capture=capture_time()) if dump_mode == "append" else DumpOptions()


def capture_time() -> datetime:
    """Return the scheduled start of the flow run in São Paulo.

    A retry of the same flow run gives the same time, so its capture is the same.
    """
    return cast("datetime", flow_run.scheduled_start_time).astimezone(ZoneInfo(TIMEZONE))


async def post_discord(webhook: str, content: str) -> str | None:
    """Post a message to a Discord webhook, cut to the limit of Discord, and return its id.

    The id is nothing when Discord answers without the message.
    Raise `HTTPStatusError` if Discord refuses it.
    """
    async with AsyncClient() as client:
        response = await client.post(webhook, params={"wait": "true"}, json={"content": content[:DISCORD_LIMIT]})
        response.raise_for_status()

    try:
        identifier = dig(read_json(response), "id")
    except ValidationError:
        return None

    return identifier if isinstance(identifier, str) else None
