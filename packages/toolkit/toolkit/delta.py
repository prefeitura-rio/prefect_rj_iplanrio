"""Write chunks into a Delta table, and know which ones did not change.

The ledger of the table holds one fingerprint for each chunk and config. A chunk whose
fingerprint is the same is skipped, and a chunk that changed is replaced in one atomic
commit. The table lives in a bucket and is read through ADC, or on a local path.
"""

from collections.abc import Collection, Sequence
from datetime import datetime
from hashlib import md5
from pathlib import Path
from time import sleep
from typing import cast

from deltalake import CommitProperties, DeltaTable, Transaction, write_deltalake
from deltalake.exceptions import CommitFailedError
from psycopg.sql import Literal
from pyarrow import RecordBatchReader, repeat, timestamp
from pyarrow import scalar as arrow_scalar
from pyarrow import schema as arrow_schema
from pyarrow.dataset import dataset

from .constants import (
    BATCH_COLUMN,
    COMMIT_ATTEMPTS,
    EXTRACTED_AT_COLUMN,
    VACUUM_RETENTION_HOURS,
)
from .executor.executor import Executor
from .telemetry import logger
from .types import Fingerprint


def hash_summary(rows: int, low: str, high: str) -> int:
    """Turn a content summary into the 60 bit number that the ledger stores.

    ```python
    hash_summary(3, "1", "2")  # 810314219720917701
    ```
    """
    digest = md5(f"{rows}:{low}:{high}".encode(), usedforsecurity=False)
    return int(digest.hexdigest()[:15], 16)


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


def write_chunk(
    uri: str,
    directory: Path,
    key: str,
    ledger: str,
    number: int,
    *,
    overwrite_schema: bool = False,
    append: bool = False,
    extracted_at: datetime | None = None,
) -> None:
    """Replace the rows of one chunk with the Parquet files of a folder.

    It runs in one commit that also records `number` in the ledger as `ledger`. An empty
    folder removes the rows of the chunk. A commit conflict, such as two writers that
    create the table, is tried again. A schema that differs from the table raises
    `SchemaMismatchError`, unless `overwrite_schema` replaces the schema of the table.
    With `append` the rows are added and the rows of the chunk stay. With
    `extracted_at` each row gets that time in the column `_prefect_extracted_at`.
    """
    properties = CommitProperties(app_transactions=[Transaction(ledger, number)])

    for attempt in range(1, COMMIT_ATTEMPTS + 1):
        reader = open_reader(directory, extracted_at)
        try:
            if append:
                write_deltalake(
                    uri,
                    reader,
                    mode="append",
                    partition_by=[BATCH_COLUMN],
                    commit_properties=properties,
                )
            else:
                write_deltalake(
                    uri,
                    reader,
                    mode="overwrite",
                    predicate=f"{BATCH_COLUMN} = '{key}'",
                    partition_by=[BATCH_COLUMN],
                    schema_mode="overwrite" if overwrite_schema else None,
                    commit_properties=properties,
                )
        except CommitFailedError:
            if attempt == COMMIT_ATTEMPTS:
                raise
            logger.info("Delta commit conflict: chunk=%s attempt=%d", key, attempt)
            sleep(attempt * 0.2)
        else:
            return


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
