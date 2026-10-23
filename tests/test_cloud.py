"""The dump against the cloud, simulated in the process.

A fake GCS with the XML API and its generation preconditions, BigQuery over a Delta table with the
rules that the real services showed, and a database in memory. Nothing here needs the network.
"""

from asyncio import gather
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from secrets import token_hex
from typing import cast

import pytest
from deltalake import DeltaTable
from fixtures import FakeDatabaseSource
from polars import col
from pyarrow import Table, decimal128
from pyarrow import array as arrow_array
from pyarrow import table as arrow_table
from pyarrow import timestamp as arrow_timestamp
from toolkit.constants import BATCH_COLUMN
from toolkit.delta import compact, maintain
from toolkit.gcs import list_keys
from toolkit.types import Chunks

from utils.delta import load_to_delta
from utils.origins import read_request

pytestmark = pytest.mark.cloud

INSTANT = datetime(2024, 3, 1, 10, tzinfo=UTC)
MONTHS: Chunks = {"column": "dia", "width": "month", "begin": "2024-01-01", "end": "2024-03-31"}


def sample(rows: list[dict[str, object]]) -> Table:
    """Return rows as an Arrow table."""
    return Table.from_pylist(rows)


SAMPLE: list[dict[str, object]] = [
    {"id": index, "nome": f" cliente {index} ", "dia": date(2024, 1 + index % 3, 1 + index % 27), "valor": index * 1.5}
    for index in range(1, 301)
]


@pytest.fixture
def database(fake_database: FakeDatabaseSource) -> FakeDatabaseSource:
    """Return the database in memory with the 300 sample orders."""
    fake_database.tables["public.orders"] = sample(SAMPLE)

    return fake_database


def name(prefix: str) -> str:
    """Return a table name that no other test of the run uses."""
    return f"{prefix}_{token_hex(2)}"


async def test_a_dump_is_read_by_bigquery_with_the_rows_of_the_source(cloud, database, dump_to_cloud):
    """Load monthly chunks, publish them, and find every row once in BigQuery."""
    destination = cloud.destination(name("load"))

    await dump_to_cloud(database, destination, chunks=MONTHS)
    await cloud.publish(destination)

    dataframe = cloud.read(destination)
    assert (dataframe.height, dataframe["id"].n_unique()) == (300, 300)
    assert sorted(dataframe[BATCH_COLUMN].unique().to_list()) == ["2024-01-01", "2024-02-01", "2024-03-01"]


async def test_the_extraction_time_is_a_timestamp_in_bigquery(cloud, database, dump_to_cloud):
    """Publish the column that says when each chunk was written, as a `TIMESTAMP`."""
    destination = cloud.destination(name("stamped"))

    await dump_to_cloud(database, destination, columns={"add_timestamp": True})
    await cloud.publish(destination)

    assert cloud.types(destination)["_prefect_extracted_at"] == "TIMESTAMP"
    assert cloud.read(destination)["_prefect_extracted_at"].null_count() == 0


async def test_the_same_dump_again_writes_no_commit_and_no_object(cloud, database, dump_to_cloud):
    """Skip every chunk that did not change, so the bucket and the log stay as they are."""
    destination = cloud.destination(name("rerun"))
    await dump_to_cloud(database, destination, chunks=MONTHS)
    before = DeltaTable(destination.uri)
    version, files = before.version(), sorted(before.file_uris())

    await dump_to_cloud(database, destination, chunks=MONTHS)

    after = DeltaTable(destination.uri)
    assert after.version() == version
    assert sorted(after.file_uris()) == files


async def test_a_changed_chunk_is_replaced_in_bigquery_without_duplicates(cloud, database, dump_to_cloud):
    """Replace only the chunks that changed, and keep one copy of every row."""
    destination = cloud.destination(name("change"))
    await dump_to_cloud(database, destination, chunks=MONTHS)
    await cloud.publish(destination)
    database.tables["public.orders"] = sample([{**SAMPLE[0], "valor": 9999.0}, *SAMPLE[1:]])

    await dump_to_cloud(database, destination, chunks=MONTHS)

    dataframe = cloud.read(destination)
    assert dataframe.filter(col("id") == 1)["valor"].to_list() == [9999.0]
    assert (dataframe.height, dataframe["id"].n_unique()) == (300, 300)


async def test_a_chunk_that_became_empty_loses_its_rows_in_bigquery(cloud, database, dump_to_cloud):
    """Expire the rows of a month that has none in the source now."""
    destination = cloud.destination(name("empty"))
    await dump_to_cloud(database, destination, chunks=MONTHS)
    await cloud.publish(destination)
    database.tables["public.orders"] = sample([row for row in SAMPLE if cast("date", row["dia"]).month != 3])

    await dump_to_cloud(database, destination, chunks=MONTHS)

    assert sorted(cloud.read(destination)[BATCH_COLUMN].unique().to_list()) == ["2024-01-01", "2024-02-01"]


async def test_overwrite_with_other_columns_reaches_bigquery_after_publish(cloud, database, dump_to_cloud):
    """Replace the schema in overwrite, refresh the table, and see only the new columns."""
    destination = cloud.destination(name("schema"))
    await dump_to_cloud(database, destination, chunks=MONTHS)
    await cloud.publish(destination)

    await dump_to_cloud(database, destination, "overwrite", chunks=MONTHS, select=["id", "dia"])
    await cloud.publish(destination)

    assert list(cloud.read(destination).columns) == ["id", "dia", BATCH_COLUMN]


async def test_bigquery_sees_the_new_schema_as_soon_as_the_files_change(cloud, database, dump_to_cloud):
    """Read the schema from the Delta log on every query, so a publish is not needed to see it."""
    destination = cloud.destination(name("detect"))
    await dump_to_cloud(database, destination, chunks=MONTHS)
    await cloud.publish(destination)

    await dump_to_cloud(database, destination, "overwrite", chunks=MONTHS, select=["id", "dia"])

    assert list(cloud.types(destination)) == ["id", "dia", BATCH_COLUMN]


async def test_publishing_twice_refreshes_the_table_instead_of_failing(cloud, database, dump_to_cloud):
    """Create the table the first time and refresh its schema the next times."""
    destination = cloud.destination(name("twice"))
    await dump_to_cloud(database, destination)

    await cloud.publish(destination)
    await cloud.publish(destination)

    assert cloud.read(destination).height == 300


async def test_the_types_of_the_pipelines_survive_delta_and_bigquery(cloud, dump_to_cloud):
    """Read integers, decimals, dates and both kinds of timestamp with the types that dbt expects."""
    destination = cloud.destination(name("types"))
    dataframe = arrow_table(
        {
            "id": [1],
            "texto": ["a"],
            "numero": [1.5],
            "decimal": arrow_array([Decimal("1.25")], decimal128(10, 2)),
            "flag": [True],
            "dia": arrow_array([date(2024, 3, 1)]),
            "instante": arrow_array([INSTANT.replace(tzinfo=None)], arrow_timestamp("us")),
            "instante_utc": arrow_array([INSTANT], arrow_timestamp("us", "UTC")),
        }
    )
    await dump_to_cloud(FakeDatabaseSource({"public.types": dataframe}), destination, table="public.types")

    await cloud.publish(destination)

    assert cloud.types(destination) == {
        "id": "INTEGER",
        "texto": "STRING",
        "numero": "FLOAT",
        "decimal": "NUMERIC",
        "flag": "BOOLEAN",
        "dia": "DATE",
        "instante": "DATETIME",
        "instante_utc": "TIMESTAMP",
        BATCH_COLUMN: "STRING",
    }


async def test_runs_that_write_the_same_table_at_the_same_time_lose_no_chunk(cloud, dump_to_cloud):
    """Let four runs commit to one table over `gs://`, and find every month in it."""
    destination = cloud.destination(name("race"))
    months = [(f"2024-{month:02d}-01", f"2024-{month:02d}-28") for month in range(1, 5)]

    async def one(begin: str, end: str) -> None:
        """Dump a single month into the shared table."""
        rows = [{**row, "dia": date.fromisoformat(begin)} for row in SAMPLE[:10]]
        chunks: Chunks = {"column": "dia", "width": "month", "begin": begin, "end": end}
        await dump_to_cloud(FakeDatabaseSource({"public.orders": sample(rows)}), destination, chunks=chunks)

    await gather(*(one(begin, end) for begin, end in months))

    chunks = cast("list[str]", DeltaTable(destination.uri).to_pyarrow_table().column(BATCH_COLUMN).to_pylist())
    assert sorted(set(chunks)) == [begin for begin, _ in months]


async def test_a_dump_leaves_a_checkpoint_and_bigquery_still_reads(cloud, database, dump_to_cloud):
    """Write a checkpoint after the chunks, and query the table that has one."""
    destination = cloud.destination(name("check"))

    await dump_to_cloud(database, destination, chunks=MONTHS)
    await cloud.publish(destination)

    keys = await list_keys(cloud.store(), f"{destination.prefix}/_delta_log")
    assert any("checkpoint" in key for key in keys)
    assert cloud.read(destination).height == 300


async def test_the_files_of_an_older_layout_go_before_an_overwrite_and_a_delta_table_stays(cloud, database):
    """Delete old Hive files in the first overwrite, and never delete the Delta table in the next."""
    destination = cloud.destination(name("legacy"))
    store = cloud.store()
    old = f"{destination.prefix}/ano=2023/part-old.parquet"
    await store.put_async(old, b"old")
    values = read_request("postgres", {"table": "public.orders"}, batched=False)

    reading = replace(database, origin=values)

    await load_to_delta(reading, destination, "overwrite", {})
    first = await list_keys(store, destination.prefix)
    await load_to_delta(reading, destination, "overwrite", {})
    second = await list_keys(store, destination.prefix)

    assert old not in first
    assert any("_delta_log" in key for key in second)
    assert DeltaTable(destination.uri).to_pyarrow_table().num_rows == 300


async def test_captures_add_up_in_bigquery_and_compacting_keeps_every_row(cloud, tmp_path, capture):
    """Add what each run brings, ignore a repeated run, and still read every row after a compaction."""
    destination = cloud.destination(name("capture"))
    first = datetime(2024, 3, 10, 9, 0, tzinfo=UTC)

    await capture(destination, tmp_path, [{"id": 1, "nome": "Ana"}], first)
    await capture(destination, tmp_path, [{"id": 2, "nome": "Bia"}], first.replace(minute=5))
    await capture(destination, tmp_path, [{"id": 9, "nome": "again"}], first.replace(minute=5))
    await capture(destination, tmp_path, [{"id": 3, "nome": "Caio"}], first.replace(day=11))
    await cloud.publish(destination)
    assert sorted(cloud.read(destination)["id"].to_list()) == [1, 2, 3]

    compact(destination.uri)
    maintain(destination.uri)
    await cloud.publish(destination)

    assert sorted(cloud.read(destination)["id"].to_list()) == [1, 2, 3]
    assert len(DeltaTable(destination.uri).file_uris()) == 2
