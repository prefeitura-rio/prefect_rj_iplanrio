"""Delta tables in the bucket: where they live, how a dump loads them, BigQuery."""

from asyncio import Lock, to_thread
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

from obstore.store import ObjectStore
from polars import DataFrame, concat
from toolkit.bigquery import BigQuery
from toolkit.chunks import (
    Chunk,
    ChunkPlan,
    LedgerEntry,
    Rows,
    build_batch_column,
    build_ledger_id,
    hash_parts,
)
from toolkit.constants import BATCH_COLUMN, EXTRACTED_AT_COLUMN
from toolkit.delta import (
    compute_fingerprint,
    maintain,
    read_known_fingerprints,
    sweep,
    table_exists,
)
from toolkit.executor.engines.base import Engine, require_origin
from toolkit.executor.engines.duckdb import DuckDBEngine
from toolkit.executor.executor import Executor
from toolkit.executor.sources.backends.bigquery import BigQuerySource
from toolkit.executor.sources.protocols import Source
from toolkit.executor.types import Origin
from toolkit.gcs import create_store, delete_keys, list_keys, list_prefixes, upload
from toolkit.telemetry import logger
from toolkit.utils import select_columns

from .constants import (
    DISCORD_LIMIT,
    STAGING_PREFIX,
)
from .modes import (
    Loader,
    Mode,
    build_ledger_key,
    cut_mode,
    pick_mode,
    pick_pending,
    pick_probe,
    pick_shard,
    skips_unchanged,
    sweeps,
)
from .types import DumpMode, DumpOptions, FileFormat, Maintenance


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

        `deltalake` reads `gcs://` with OpenDAL, which accepts a WIF ADC
        (`external_account`), and `gs://` with `object_store`, which does not.
        `BigQuery` turns it into `gs://`.
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
    """Create the BigQuery table over the Delta table of the bucket, or refresh it.

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


def hash_config(origin: Origin, options: DumpOptions, probe_label: str = "") -> str:
    """Hash everything that changes the bytes of a chunk.

    That is what is read, and the filters, the columns and the normalization. The range
    of the chunks and `lookback` are left out: they change which chunks run, not what
    a chunk holds. A capture or a merge leaves out what is read too, because its files
    are in a new folder on every run. The key of a merge is part of the config. So is
    the label of the probe (its kind and column), when a probe runs: the ledger then
    holds its tokens and not the fingerprints of the content, and a new label loads
    every chunk again.
    """
    spec = options.get("chunks")
    fresh = "capture" in options or "merge" in options
    parts: dict[str, object] = {
        "read": None if fresh else {"origin": type(origin).__name__, **asdict(origin)},
        "filters": options.get("filters"),
        "select": options.get("select"),
        "columns": options.get("columns"),
        "chunks": spec and {"column": spec["column"], "width": spec["width"]},
    }

    if "merge" in options:
        parts["merge"] = options["merge"]

    if probe_label:
        parts["probe"] = probe_label

    return hash_parts(parts)


async def finish_dump(uri: str, mode: Mode, *, written: bool = True) -> None:
    """Close a dump of a table: delete the chunks that left the range, then maintain it.

    An `overwrite` sweeps the chunks outside its range, and a table that the dump wrote
    gets a checkpoint and loses the files that no version reads. A dump in shards ends
    here once, after all of its shards.
    """
    if sweeps(mode):
        await to_thread(sweep, uri, [chunk.key for chunk in cut_mode(mode)])

    if written:
        await to_thread(maintain, uri)


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
    it reads at the same time, and the chunks are written to the table together: the
    ledger gives each chunk its own transaction, so two commits never clash. A run with
    a `shard` loads only its slice of the chunks (see `pick_shard`), and leaves the
    sweep and the maintenance to `finish_dump`. A `capture` is the exception: its rows
    are added to the table, in the chunk of its day, and a capture that the ledger
    knows is never written again. `merge` is one chunk too: its rows are matched to the
    table by key and only the new or changed ones are written, so a rerun or an
    overlapping window adds no duplicate. Raise `ValueError` for a source with no
    origin, for a capture with `chunks` or with `overwrite`, for a mode that does not
    fit its options (see `pick_mode`), and for `filters` that are not one predicate
    each.

    The origin is what to read, as `read_request` or a flow builds it.
    """
    origin = require_origin(source)
    spec = options.get("chunks")
    columns = options.get("columns", {})
    filters = options.get("filters", [])

    mode = pick_mode(dump_mode, options)
    probe = pick_probe(source, mode, options)
    config = hash_config(origin, options, probe.label if probe else "")
    cut = cut_mode(mode)
    ids = {chunk.key: build_ledger_id(config, build_ledger_key(mode, chunk)) for chunk in cut}
    known = await to_thread(read_known_fingerprints, uri, list(ids.values()))
    seen = {key for key, name in ids.items() if name in known}
    todo = pick_pending(mode, cut, seen)
    shard = options.get("shard")

    if shard:
        mine = {chunk.key for chunk in pick_shard(cut, shard)}
        todo = [chunk for chunk in todo if chunk.key in mine]

    extracted_at = datetime.now(UTC) if columns.get("add_timestamp") else None
    loader = Loader(mode, uri, extracted_at)
    written = skipped = 0
    fingerprinting = Lock()

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

        def plan(chunk: Chunk) -> ChunkPlan:
            """Return the rows, the columns and the ledger number of one chunk."""
            rows = Rows(filters, spec["column"] if spec else None, chunk if spec else None)
            return ChunkPlan(rows, [*base, build_batch_column(chunk.key)], known.get(ids[chunk.key]))

        async def handle(chunk: Chunk, folder: Path, token: int | None) -> None:
            """Write a downloaded chunk, with its token or its fingerprint."""
            nonlocal written, skipped
            number = token

            if number is None:
                async with fingerprinting:
                    number = await compute_fingerprint(Executor(local), folder)

                if skips_unchanged(mode) and known.get(ids[chunk.key]) == number:
                    skipped += 1
                    return

            await to_thread(loader.write, folder, chunk, LedgerEntry(ids[chunk.key], number))
            written += 1

        skipped += len(await executor.run_chunks(todo, plan, handle, probe=probe))

    if not shard:
        await finish_dump(uri, mode, written=written > 0)

    logger.info(
        "Delta dump finished: uri=%s chunks=%d todo=%d written=%d skipped=%d probe=%s shard=%s",
        uri,
        len(cut),
        len(todo),
        written,
        skipped,
        probe.label if probe else "none",
        f"{shard['index']}/{shard['count']}" if shard else "none",
    )


def bigquery_source(project: str, origin: Origin) -> Source:
    """Return the BigQuery source of a project that reads an origin."""
    return BigQuerySource(project=project, origin=origin)


async def clear_legacy(destination: Destination, dump_mode: DumpMode) -> None:
    """Delete the files of an older layout before an `overwrite` builds a Delta table.

    A Delta table is never deleted: `overwrite` rewrites its chunks instead.
    """
    if dump_mode == "overwrite" and not await to_thread(table_exists, destination.uri):
        await clear_prefix(create_store(destination.bucket), destination)


async def load_to_delta(
    source: Source,
    destination: Destination,
    dump_mode: DumpMode,
    options: DumpOptions | None,
) -> None:
    """Dump a source into the Delta table of a destination.

    An `overwrite` first deletes the files of an older layout, except in a `shard`: the
    parent run does it once. See `dump_to_delta`.
    """
    chosen = options or {}

    if "shard" not in chosen:
        await clear_legacy(destination, dump_mode)

    await dump_to_delta(source, destination.uri, dump_mode, chosen)


def without_chunks(options: DumpOptions | None, source: str) -> DumpOptions:
    """Return the source options that DuckDB cannot filter and that are not chunked.

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
    """Copy the rows of a source to Parquet files in the bucket, in batches.

    `batch_size` is the rows in a batch.

    It reads the origin of the source and uploads each batch, so memory holds one batch.
    This is plain batching: a rerun in `append` adds the rows again. `overwrite` deletes
    the old files first. Raise `ValueError` for `merge`, which needs a Delta table.
    """
    if dump_mode == "merge":
        raise ValueError("A copy in batches cannot merge: use a Delta dump")

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
