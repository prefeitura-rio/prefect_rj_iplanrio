"""Write chunks into a Delta table, merge rows by key, and skip unchanged chunks.

The ledger of the table holds one fingerprint for each chunk and config. A chunk whose
fingerprint is the same is skipped, and a chunk that changed is replaced in one atomic
commit. The table lives in a bucket and is read through ADC, or on a local path.
"""

from collections.abc import Collection, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from re import fullmatch
from time import sleep
from typing import cast

from deltalake import CommitProperties, DeltaTable, Transaction, write_deltalake
from deltalake.exceptions import CommitFailedError, SchemaMismatchError
from deltalake.transaction import PostCommitHookProperties
from psycopg.sql import Literal
from pyarrow import RecordBatchReader, Table, repeat, timestamp
from pyarrow import scalar as arrow_scalar
from pyarrow import schema as arrow_schema
from pyarrow import table as arrow_table
from pyarrow.dataset import Dataset, dataset

from .chunks import LedgerEntry, hash_summary
from .constants import (
    ALL_KEY,
    BATCH_COLUMN,
    COMMIT_ATTEMPTS,
    COUNT_COLUMN,
    DATE_KEY,
    EXTRACTED_AT_COLUMN,
    NUMBER_KEY,
    SOURCE_ALIAS,
    TARGET_ALIAS,
    VACUUM_RETENTION_HOURS,
)
from .executor.executor import Executor
from .telemetry import logger
from .types import Fingerprint, MergeReport, MergeSpec, TableReport, WriteFlags


async def compute_fingerprint(executor: Executor, directory: Path) -> int:
    """Return the fingerprint of the Parquet files of a folder.

    It follows the content and ignores the order of the rows, and it changes when a row
    is added, removed, repeated or changed.
    """
    data = await executor.query(
        "duckdb/fingerprint",
        {"source": Literal(f"{directory}/*.parquet")},
        expect=Fingerprint,
    )
    row = data.dataframe.row(0, named=True)
    return hash_summary(
        cast("int", row["n"]), cast("str", row["low"]), cast("str", row["high"])
    )


def read_known_fingerprints(uri: str, ids: Sequence[str]) -> dict[str, int]:
    """Return the fingerprint that the ledger holds for each of the ids that it knows.

    A table that does not exist yet knows nothing.
    """
    if not DeltaTable.is_deltatable(uri):
        return {}

    table = DeltaTable(uri)
    return {
        name: version
        for name in ids
        if (version := table.transaction_version(name)) is not None
    }


def open_reader(directory: Path, extracted_at: datetime | None) -> RecordBatchReader:
    """Stream the Parquet files of a folder, with `extracted_at` on each row if set."""
    reader = dataset(directory, format="parquet").scanner().to_reader()
    if extracted_at is None:
        return reader

    stamp = arrow_scalar(extracted_at, type=timestamp("us", "UTC"))
    schema = arrow_schema(
        [
            *zip(reader.schema.names, reader.schema.types, strict=True),
            (EXTRACTED_AT_COLUMN, stamp.type),
        ]
    )

    return RecordBatchReader.from_batches(
        schema,
        (
            batch.append_column(EXTRACTED_AT_COLUMN, repeat(stamp, batch.num_rows))
            for batch in reader
        ),
    )


def checkpointing() -> PostCommitHookProperties:
    """Make a commit write a checkpoint when its version reaches the table interval.

    A table that many commits fill, such as a dump that loads a chunk each, keeps the
    log that a write reads short. Delta checks the interval, 100 commits by default.
    """
    return PostCommitHookProperties(create_checkpoint=True)


def write_chunk(
    uri: str,
    directory: Path,
    key: str,
    entry: LedgerEntry,
    flags: WriteFlags | None = None,
) -> None:
    """Replace the rows of one chunk with the Parquet files of a folder.

    It runs in one commit that also records the `entry` in the ledger. An empty folder
    removes the rows of the chunk. A commit conflict, such as two writers that create
    the table, is tried again. A schema that differs from the table raises
    `SchemaMismatchError`, unless `overwrite_schema` replaces the schema of the table.
    With `append` the rows are added and the rows of the chunk stay. With
    `extracted_at` each row gets that time in the column `_prefect_extracted_at`.
    """
    chosen = flags or {}
    properties = CommitProperties(
        app_transactions=[Transaction(entry.id, entry.number)]
    )

    for attempt in range(1, COMMIT_ATTEMPTS + 1):
        reader = open_reader(directory, chosen.get("extracted_at"))
        try:
            if chosen.get("append"):
                write_deltalake(
                    uri,
                    reader,
                    mode="append",
                    commit_properties=properties,
                    post_commithook_properties=checkpointing(),
                )
            else:
                write_deltalake(
                    uri,
                    reader,
                    mode="overwrite",
                    predicate=f"{BATCH_COLUMN} = '{key}'",
                    schema_mode="overwrite" if chosen.get("overwrite_schema") else None,
                    commit_properties=properties,
                    post_commithook_properties=checkpointing(),
                )
        except CommitFailedError:
            if attempt == COMMIT_ATTEMPTS:
                raise
            logger.info("Delta commit conflict: chunk=%s attempt=%d", key, attempt)
            sleep(attempt * 0.2)
        else:
            return


def quote_name(name: str) -> str:
    """Quote a column name for a predicate."""
    return '"' + name.replace('"', '""') + '"'


def build_literal(value: str | float | date) -> str:
    """Return a value as a SQL literal that a predicate compares with its column.

    ```python
    build_literal("o'brien")  # "'o''brien'"
    build_literal(date(2026, 10, 22))  # "CAST('2026-10-22' AS DATE)"
    ```
    """
    if isinstance(value, datetime):
        return f"CAST('{value.isoformat(sep=' ')}' AS TIMESTAMP)"
    if isinstance(value, date):
        return f"CAST('{value.isoformat()}' AS DATE)"
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, int | float):
        return str(value)
    return "'" + value.replace("'", "''") + "'"


def build_merge_predicate(spec: MergeSpec, low: str | float | date | None) -> str:
    """Return the predicate that matches a row of the table to a row of the new data.

    The key columns are equal. With `prune` and a `low` value, the table side must also
    reach `low`, so the merge reads only the recent files.

    ```python
    build_merge_predicate({"key": ["id"]}, None)  # 't."id" = s."id"'
    ```
    """
    terms = [
        f"{TARGET_ALIAS}.{quote_name(name)} = {SOURCE_ALIAS}.{quote_name(name)}"
        for name in spec["key"]
    ]
    prune = spec.get("prune")

    if prune and low is not None:
        terms.append(f"{TARGET_ALIAS}.{quote_name(prune)} >= {build_literal(low)}")

    return " AND ".join(terms)


def build_changed_predicate(columns: Sequence[str]) -> str:
    """Return the predicate that is true when a matched row differs in a column."""
    return " OR ".join(
        f"({TARGET_ALIAS}.{quote_name(name)} IS DISTINCT FROM {SOURCE_ALIAS}.{quote_name(name)})"
        for name in columns
    )


def count_repeated_keys(source: Table, key: Sequence[str]) -> int:
    """Return how many keys appear in more than one row of the table."""
    counts = (
        source.select(list(key)).group_by(list(key)).aggregate([([], COUNT_COLUMN)])
    )
    return sum(
        1 for rows in cast("list[int]", counts[COUNT_COLUMN].to_pylist()) if rows > 1
    )


def count_null_keys(source: Table, key: Sequence[str]) -> int:
    """Return how many rows have a NULL in any of the key columns."""
    return sum(source[name].null_count for name in key)


def check_merge_source(source: Table, key: Sequence[str]) -> None:
    """Raise `ValueError` for a NULL in the key, or for a key that two rows share."""
    nulls = count_null_keys(source, key)
    if nulls:
        raise ValueError(
            f"The new data has {nulls} NULL values in {key}: merge needs a full key"
        )

    repeated = count_repeated_keys(source, key)
    if repeated:
        raise ValueError(
            f"The new data repeats {repeated} keys of {key}: merge needs one row for each"
        )


def check_merge_schema(source: Table, table: DeltaTable) -> None:
    """Raise `SchemaMismatchError` for other columns than the table."""
    different = set(source.column_names) ^ {
        field.name for field in table.schema().fields
    }

    if different:
        raise SchemaMismatchError(
            f"The columns differ from the table: {sorted(different)}"
        )


def find_lowest(source: Table, column: str | None) -> str | float | date | None:
    """Return the lowest value of a column, or nothing without a column or a value."""
    if column is None:
        return None

    values = cast("list[str | float | date]", source[column].drop_null().to_pylist())
    return min(values, default=None)


def run_merge(
    table: DeltaTable,
    source: Table,
    spec: MergeSpec,
    properties: CommitProperties,
) -> dict[str, int]:
    """Run the merge of the new rows into the table, and return its metrics."""
    key = spec["key"]
    merger = table.merge(
        source,
        predicate=build_merge_predicate(spec, find_lowest(source, spec.get("prune"))),
        source_alias=SOURCE_ALIAS,
        target_alias=TARGET_ALIAS,
        commit_properties=properties,
        post_commithook_properties=checkpointing(),
    ).when_not_matched_insert_all()
    changing = [
        name
        for name in source.column_names
        if name not in {*key, BATCH_COLUMN, EXTRACTED_AT_COLUMN}
    ]

    if spec.get("update", True) and changing:
        merger = merger.when_matched_update_all(
            predicate=build_changed_predicate(changing)
        )

    return cast("dict[str, int]", merger.execute())


def merge_chunk(
    uri: str,
    directory: Path,
    spec: MergeSpec,
    entry: LedgerEntry,
    extracted_at: datetime | None = None,
) -> MergeReport:
    """Merge the rows of a folder of Parquet files into the table by key, in one commit.

    A key that the table lacks is added. A matched row changes only when `update` is on
    and a column differs, so the same data merged twice rewrites no file. The `entry` is
    the ledger entry of the run, committed with the rows. The first run creates the
    table, and a folder with no rows changes nothing. The new data must fit in memory
    and hold one row for each key, with no NULL in it: otherwise `ValueError`, because
    Delta fails an update, inserts both rows, or never matches a NULL. Other columns
    than the ones of the table raise `SchemaMismatchError`, as an append does. With
    `extracted_at` each row gets that time in the column `_prefect_extracted_at`.
    Return the rows, and how many were added and changed, and how many files were read.
    """
    source = open_reader(directory, extracted_at).read_all()
    report = MergeReport(rows=source.num_rows, inserted=0, updated=0, scanned=0)

    if source.num_rows == 0:
        logger.info("Delta merge skipped: rows=0")
        return report

    check_merge_source(source, spec["key"])
    properties = CommitProperties(
        app_transactions=[Transaction(entry.id, entry.number)]
    )

    if DeltaTable.is_deltatable(uri):
        table = DeltaTable(uri)
        check_merge_schema(source, table)
        metrics = run_merge(table, source, spec, properties)
        report["inserted"] = metrics["num_target_rows_inserted"]
        report["updated"] = metrics["num_target_rows_updated"]
        report["scanned"] = metrics["num_target_files_scanned"]
    else:
        write_deltalake(
            uri,
            source,
            mode="append",
            commit_properties=properties,
            post_commithook_properties=checkpointing(),
        )
        report["inserted"] = source.num_rows

    logger.info(
        "Delta merge finished: rows=%d inserted=%d updated=%d scanned=%d",
        report["rows"],
        report["inserted"],
        report["updated"],
        report["scanned"],
    )
    return report


def sweep(uri: str, keep: Collection[str]) -> None:
    """Delete every chunk not in `keep`. A table that does not exist is left."""
    if not DeltaTable.is_deltatable(uri):
        return

    keys = ", ".join(f"'{key}'" for key in keep)
    DeltaTable(uri).delete(f"{BATCH_COLUMN} NOT IN ({keys})")


def compact(uri: str) -> None:
    """Merge the small files of each chunk into larger ones, in one commit."""
    DeltaTable(uri).optimize.compact()


def maintain(uri: str) -> None:
    """Write a checkpoint and remove the files that no version reads any more.

    BigQuery replays the log of the table, so a checkpoint keeps its queries fast.
    """
    table = DeltaTable(uri)
    table.create_checkpoint()
    table.vacuum(retention_hours=VACUUM_RETENTION_HOURS, dry_run=False)


def inspect_table(uri: str) -> TableReport:
    """Read the version, the files, the bytes, the last commit and the chunk keys.

    Raise `TableNotFoundError` if no Delta table lives at `uri`.
    """
    table = DeltaTable(uri)
    sizes = cast(
        "list[int]", arrow_table(table.get_add_actions())["size_bytes"].to_pylist()
    )
    source = cast("Dataset", table.to_pyarrow_dataset())  # pyright: ignore[reportUnknownMemberType]  # deltalake types it loosely
    chunks = source.to_table(columns=[BATCH_COLUMN])
    keys = cast("list[str]", chunks.column(BATCH_COLUMN).unique().to_pylist())
    stamp = cast("int", table.history(1)[0]["timestamp"])

    return TableReport(
        version=table.version(),
        files=len(sizes),
        bytes=sum(sizes),
        last_commit=datetime.fromtimestamp(stamp / 1000, UTC),
        keys=sorted(set(keys)),
    )


def find_layout_problems(keys: Collection[str]) -> list[str]:
    """Name what the chunk keys of a table say about duplicated rows.

    A run in `append` replaces only the chunks that it cuts. If the cut changes, the
    rows of the old chunks stay next to the new ones. The `all` chunk next to other
    chunks, and dates next to numbers, mark that.

    ```python
    find_layout_problems(["all", "2024-03-01"])  # ["all next to chunk keys"]
    find_layout_problems(["2024-03-01", "null"])  # []
    ```
    """
    problems: list[str] = []

    if ALL_KEY in keys and len(keys) > 1:
        problems.append("all next to chunk keys")

    has_dates = any(fullmatch(DATE_KEY, key) for key in keys)
    has_numbers = any(fullmatch(NUMBER_KEY, key) for key in keys)
    if has_dates and has_numbers:
        problems.append("dates next to numbers")

    return problems
