from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from json import loads
from pathlib import Path
from re import compile as compile_pattern
from types import SimpleNamespace
from typing import Unpack, cast

import pytest
from deltalake import DeltaTable
from deltalake.exceptions import SchemaMismatchError
from duckdb import connect as duckdb_connect
from fixtures import ConcurrentFilesSource, ItemData, ItemDict, ItemModel
from httpx2 import ConnectError, ConnectTimeout, HTTPStatusError, Request, Response
from pyarrow import Table, decimal128
from pyarrow import array as arrow_array
from pyarrow import table as arrow_table
from pyarrow import timestamp as arrow_timestamp
from pyarrow.parquet import write_table
from toolkit import gcs
from toolkit.constants import BATCH_COLUMN
from toolkit.dbt import DbtRun
from toolkit.executor.requests import Changes, Files, Query
from toolkit.executor.requests import Table as TableOrigin
from toolkit.executor.sources.backends.files import FilesSource
from toolkit.types import Chunks

from utils import utils
from utils.constants import DISCORD_LIMIT
from utils.types import DbtNode, DbtOutcome, DumpMode, DumpOptions, Maintenance
from utils.utils import (
    Destination,
    capture_time,
    clear_legacy,
    clear_prefix,
    discover_tables,
    dump_in_batches,
    dump_to_delta,
    epsg_code,
    extract_records,
    failure_message,
    files_glob,
    is_transient,
    load_records_schema,
    load_to_delta,
    publish,
    read_dataframe,
    read_nodes,
    read_request,
    summarize_dbt,
    summarize_maintenance,
    validate_records,
    without_chunks,
    write_json_lines,
)

DESTINATION = Destination("b", "ds", "tb")


def test_a_destination_names_the_files_and_the_external_table():
    """Name the folder, the URI, the dataset and the table of one BigQuery table."""
    assert DESTINATION.prefix == "staging/ds/tb"
    assert DESTINATION.uri == "gcs://b/staging/ds/tb"
    assert DESTINATION.dataset("p") == "p.ds_staging"
    assert DESTINATION.table("p") == "p.ds_staging.tb"


async def test_regression_clearing_a_table_leaves_a_similar_name_alone(store, put):
    """Regression: the folder of `tb` must not match the files of `tb2`."""
    await put(store, "staging/ds/tb/old.parquet", "staging/ds/tb2/keep.parquet")

    await clear_prefix(store, DESTINATION)

    assert await gcs.list_keys(store, "staging/ds") == ["staging/ds/tb2/keep.parquet"]


@pytest.mark.parametrize(
    ("is_folder", "file_format", "expected"),
    [
        pytest.param(False, "csv", "{root}/in.csv", id="a_file_is_read_as_it_is"),
        pytest.param(True, "json", "{root}/**/*.json", id="a_folder_is_read_by_glob"),
    ],
)
def test_the_path_of_the_files_to_read_is_a_file_or_a_glob(tmp_path: Path, is_folder, file_format, expected):
    """Read one file as it is, and every file of one format in a folder."""
    path = tmp_path if is_folder else tmp_path / "in.csv"

    assert files_glob(path, file_format) == expected.format(root=tmp_path)


def status_error(status: int) -> HTTPStatusError:
    """Return the error that `raise_for_status` gives for a status code."""
    request = Request("GET", "https://x")
    response = Response(status, request=request)
    return HTTPStatusError("failed", request=request, response=response)


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        pytest.param(status_error(500), True, id="a_server_error"),
        pytest.param(status_error(503), True, id="a_service_that_is_down"),
        pytest.param(status_error(429), True, id="too_many_requests"),
        pytest.param(status_error(404), False, id="a_missing_page_stays_missing"),
        pytest.param(status_error(401), False, id="a_wrong_token_stays_wrong"),
        pytest.param(ConnectTimeout("slow"), True, id="a_timeout"),
        pytest.param(ConnectError("down"), True, id="a_network_error"),
        pytest.param(ValueError("bug"), False, id="a_bug_is_not_a_transient_error"),
        pytest.param(None, False, id="no_error"),
    ],
)
def test_only_a_transient_error_is_sent_again(error, expected):
    """Send a request again after a timeout, a network error, a 429 or a 5xx."""
    assert is_transient(error) is expected


@pytest.mark.parametrize(
    ("url", "tail"),
    [
        pytest.param("https://p/runs/1", "\nhttps://p/runs/1", id="with_a_link"),
        pytest.param(None, "boom", id="without_a_link"),
    ],
)
def test_the_failure_message_names_the_flow_the_run_and_the_reason(url, tail):
    """Name the flow and the run, give the reason, and add the link if there is one."""
    message = failure_message("dump", "orders", "boom", url)

    assert message.startswith(":red_circle: **dump** `orders` failed: boom")
    assert message.endswith(tail)


def test_regression_a_long_reason_never_goes_over_the_limit_of_discord():
    """Regression: Discord refuses a message of more than 2000 characters."""
    message = failure_message("f", "r", "x" * 5000, "https://p")

    assert len(message) == DISCORD_LIMIT


ROWS = [
    {"id": 1, "nome": " Ana ", "dia": date(2024, 3, 1), "valor": 10.0},
    {"id": 2, "nome": "Bia", "dia": date(2024, 3, 1), "valor": 20.0},
    {"id": 3, "nome": "Caio", "dia": date(2024, 3, 2), "valor": 30.0},
    {"id": 4, "nome": "Dani", "dia": date(2024, 3, 3), "valor": 40.0},
]
NULL_CHUNK = 1  # a dump with `chunks` also has the chunk of the rows with no value
DAYS: Chunks = {"column": "dia", "width": "day", "begin": "2024-03-01", "end": "2024-03-03"}


class Lake:
    """A folder that stands in for the source, and a local Delta table for the bucket."""

    def __init__(self, root: Path) -> None:
        """Keep the paths of the source files and of the table."""
        self.source = root / "source"
        self.source.mkdir()
        self.uri = str(root / "table")

    def put(self, rows: Sequence[Mapping[str, object]]) -> None:
        """Replace the rows of the source."""
        write_table(Table.from_pylist(rows), self.source / "rows.parquet")

    def put_expression(self, expression: str) -> None:
        """Replace the source with one row, `id` 1 and a column `valor` that DuckDB computes."""
        with duckdb_connect() as connection:
            connection.execute(
                f"COPY (SELECT 1 AS id, {expression} AS valor) TO '{self.source / 'rows.parquet'}' (FORMAT parquet)"
            )

    async def dump(self, dump_mode: DumpMode = "append", **options: Unpack[DumpOptions]) -> int:
        """Dump the source and return how many commits the table has."""
        values = Files(f"{self.source}/*.parquet", "parquet")
        await dump_to_delta(FilesSource(origin=values), self.uri, dump_mode, options)
        return DeltaTable(self.uri).version() + 1

    def writes(self) -> int:
        """Return how many commits wrote data."""
        return sum(1 for entry in DeltaTable(self.uri).history() if entry.get("operation") == "WRITE")

    def rows(self) -> list[tuple[object, ...]]:
        """Return the rows of the table, sorted by id, without the chunk column."""
        table = DeltaTable(self.uri).to_pyarrow_table().drop(["batch"])
        return sorted((tuple(row.values()) for row in table.to_pylist()), key=lambda r: r[0])


@pytest.fixture
def lake(tmp_path: Path) -> Lake:
    """Return a lake with the four sample rows in the source."""
    lake = Lake(tmp_path)
    lake.put(ROWS)
    return lake


async def test_the_first_dump_loads_every_chunk(lake):
    """Write each day as its own chunk, and normalize the names of the columns."""
    commits = await lake.dump(chunks=DAYS)

    assert commits == 3 + NULL_CHUNK
    assert [row[0] for row in lake.rows()] == [1, 2, 3, 4]
    assert sorted(cast("list[str]", DeltaTable(lake.uri).to_pyarrow_table().column("batch").to_pylist())) == [
        "2024-03-01",
        "2024-03-01",
        "2024-03-02",
        "2024-03-03",
    ]


async def test_the_same_data_is_never_written_again(lake):
    """Skip every chunk whose fingerprint is in the ledger, so the table gets no commit."""
    first = await lake.dump(chunks=DAYS)

    second = await lake.dump(chunks=DAYS)

    assert second == first


async def test_a_changed_chunk_is_replaced_and_the_others_are_left(lake):
    """Commit once for the one day that changed, and keep the other days as they are."""
    first = await lake.dump(chunks=DAYS)
    lake.put([*ROWS[:2], {**ROWS[2], "valor": 99.0}, ROWS[3]])

    second = await lake.dump(chunks=DAYS)

    assert second == first + 1
    assert next(row for row in lake.rows() if row[0] == 3)[3] == 99.0
    assert len(lake.rows()) == 4


async def test_a_chunk_that_became_empty_loses_its_rows(lake):
    """Expire the rows of a day that has none in the source now."""
    await lake.dump(chunks=DAYS)
    lake.put(ROWS[:3])

    await lake.dump(chunks=DAYS)

    assert [row[0] for row in lake.rows()] == [1, 2, 3]


async def test_regression_rows_with_no_value_in_the_chunk_column_are_loaded(lake):
    """Regression: a real table had NULL dates, and the chunks left them out silently."""
    lake.put([*ROWS, {"id": 5, "nome": "Eli", "dia": None, "valor": 50.0}])

    await lake.dump(chunks=DAYS)

    assert [row[0] for row in lake.rows()] == [1, 2, 3, 4, 5]
    batch = DeltaTable(lake.uri).to_pyarrow_table().to_pylist()
    assert next(row for row in batch if row["id"] == 5)["batch"] == "null"


async def test_the_day_of_a_timestamp_with_a_time_zone_is_the_day_in_sao_paulo(lake):
    """Cut the days of an instant in São Paulo, as `to_sao_paulo` does, wherever the dump runs."""
    zone = arrow_timestamp("us", tz="UTC")
    write_table(
        arrow_table(
            {
                "id": [1, 2, 3],
                "instante": arrow_array(
                    [
                        datetime(2024, 3, 1, 1, 0, tzinfo=UTC),
                        datetime(2024, 3, 1, 23, 30, tzinfo=UTC),
                        datetime(2024, 3, 2, 2, 0, tzinfo=UTC),
                    ],
                    zone,
                ),
            }
        ),
        lake.source / "rows.parquet",
    )

    await lake.dump(chunks={"column": "instante", "width": "day", "begin": "2024-02-28", "end": "2024-03-03"})

    table = DeltaTable(lake.uri).to_pyarrow_table().to_pylist()
    assert {row["id"]: row["batch"] for row in table} == {1: "2024-02-29", 2: "2024-03-01", 3: "2024-03-01"}


async def test_the_rows_with_no_value_change_with_the_source(lake):
    """Replace the NULL chunk like any other, so a removed row goes away."""
    lake.put([*ROWS, {"id": 5, "nome": "Eli", "dia": None, "valor": 50.0}])
    await lake.dump(chunks=DAYS)

    lake.put(ROWS)
    await lake.dump(chunks=DAYS)

    assert [row[0] for row in lake.rows()] == [1, 2, 3, 4]


async def test_filters_and_select_narrow_the_rows_and_the_columns(lake):
    """Keep the rows that pass the filters, and only the columns that were asked for."""
    await lake.dump(
        chunks=DAYS,
        filters=["valor > 15"],
        select=["id", "dia"],
    )

    table = DeltaTable(lake.uri).to_pyarrow_table()
    assert table.column_names == ["id", "dia", "batch"]
    assert sorted(cast("list[int]", table.column("id").to_pylist())) == [2, 3, 4]


@pytest.mark.parametrize(
    ("filters", "ids"),
    [
        pytest.param(["valor > 15", "id < 4"], [2, 3], id="filters_join_with_and"),
        pytest.param(["id = 1 OR id = 4"], [1, 4], id="an_or_stays_inside_its_filter"),
        pytest.param(["id = 1 OR id = 4", "valor > 15"], [4], id="an_or_does_not_reach_the_next_filter"),
    ],
)
async def test_filters_are_where_statements_joined_by_and(lake, filters, ids):
    """Read a filter as one predicate of the source, and join the filters with AND."""
    await lake.dump(filters=filters)

    assert [row[0] for row in lake.rows()] == ids


@pytest.mark.parametrize(
    ("expression", "arrow_type", "value"),
    [
        pytest.param("TIME '10:30:00'", "string", "10:30:00", id="time_as_text"),
        pytest.param("TIMETZ '10:30:00+00'", "string", "10:30:00+00", id="time_with_zone_as_text"),
        pytest.param(
            "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID", "string", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", id="uuid"
        ),
        pytest.param("200::UTINYINT", "int16", 200, id="unsigned_tiny"),
        pytest.param("60000::USMALLINT", "int32", 60000, id="unsigned_small"),
        pytest.param("4000000000::UINTEGER", "int64", 4_000_000_000, id="unsigned_int"),
        pytest.param(
            "18446744073709551615::UBIGINT", "decimal128(20, 0)", Decimal(18446744073709551615), id="unsigned_big"
        ),
        pytest.param("INTERVAL 1 DAY", "string", "1 day", id="interval_as_text"),
    ],
)
async def test_regression_a_type_that_delta_lacks_arrives_as_text_or_a_wider_number(
    lake, expression, arrow_type, value
):
    """Regression: real databases gave `TIME`, `UUID` and unsigned numbers, which `deltalake` refused."""
    lake.put_expression(expression)

    await lake.dump()

    table = DeltaTable(lake.uri).to_pyarrow_table()
    assert str(table.schema.field("valor").type) == arrow_type
    assert table.column("valor").to_pylist() == [value]


FIRST_RUN = datetime(2024, 3, 10, 9, 0, tzinfo=UTC)
SECOND_RUN = datetime(2024, 3, 10, 9, 5, tzinfo=UTC)
NEXT_DAY_RUN = datetime(2024, 3, 11, 9, 0, tzinfo=UTC)


async def test_a_capture_adds_its_rows_and_keeps_the_ones_of_the_earlier_captures(lake):
    """Add what each run brings, so that the history of an API stays in the table."""
    lake.put(ROWS[:2])
    await lake.dump(capture=FIRST_RUN)
    lake.put(ROWS[2:])
    await lake.dump(capture=SECOND_RUN)

    assert [row[0] for row in lake.rows()] == [1, 2, 3, 4]


async def test_regression_the_same_capture_again_adds_nothing_even_if_the_source_changed(lake):
    """Regression: a retry that finds new data must not add the rows of its first try twice."""
    await lake.dump(capture=FIRST_RUN)
    commits = await lake.dump(capture=FIRST_RUN)
    lake.put([*ROWS, {"id": 5, "nome": "Eli", "dia": date(2024, 3, 3), "valor": 50.0}])

    assert await lake.dump(capture=FIRST_RUN) == commits
    assert [row[0] for row in lake.rows()] == [1, 2, 3, 4]


async def test_regression_a_capture_from_another_folder_is_still_the_same_capture(lake, tmp_path):
    """Regression: a rerun of a flow runs in a new temp folder, and must not add the rows twice."""
    await lake.dump(capture=FIRST_RUN)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    write_table(Table.from_pylist(ROWS), elsewhere / "rows.parquet")

    await dump_to_delta(
        FilesSource(origin=Files(f"{elsewhere}/*.parquet", "parquet")),
        lake.uri,
        "append",
        {"capture": FIRST_RUN},
    )

    assert [row[0] for row in lake.rows()] == [1, 2, 3, 4]


async def test_a_capture_is_in_the_chunk_of_its_day(lake):
    """Name the chunk after the day of the capture, so a day is a partition."""
    await lake.dump(capture=FIRST_RUN)
    await lake.dump(capture=SECOND_RUN)
    await lake.dump(capture=NEXT_DAY_RUN)

    batch = {row["batch"] for row in DeltaTable(lake.uri).to_pyarrow_table().to_pylist()}
    assert batch == {"2024-03-10", "2024-03-11"}


@pytest.mark.parametrize(
    ("dump_mode", "options"),
    [
        pytest.param("overwrite", {}, id="overwrite"),
        pytest.param("append", {"chunks": DAYS}, id="chunks"),
    ],
)
async def test_a_capture_refuses_what_would_replace_the_earlier_ones(lake, dump_mode, options):
    """Raise before any write, because a capture only adds."""
    with pytest.raises(ValueError, match="capture"):
        await lake.dump(dump_mode, capture=FIRST_RUN, **options)

    assert not DeltaTable.is_deltatable(lake.uri)


async def test_the_extraction_time_is_added_and_only_a_changed_chunk_gets_a_new_one(lake, stamps):
    """Keep the time of a chunk that did not change, and stamp the one that was replaced."""
    await lake.dump(chunks=DAYS, columns={"add_timestamp": True})
    first = stamps(lake)

    lake.put([ROWS[0], ROWS[1], {**ROWS[2], "valor": 99.0}, ROWS[3]])
    await lake.dump(chunks=DAYS, columns={"add_timestamp": True})
    second = stamps(lake)

    assert (second[1], second[2], second[4]) == (first[1], first[2], first[4])
    assert second[3] > first[3]


async def test_the_extraction_time_does_not_make_a_chunk_look_changed(lake):
    """Write no commit on a rerun, because the time is not part of the fingerprint."""
    commits = await lake.dump(chunks=DAYS, columns={"add_timestamp": True})

    assert await lake.dump(chunks=DAYS, columns={"add_timestamp": True}) == commits


async def test_a_source_column_with_the_name_of_the_time_is_refused(lake):
    """Raise, because the dump would write two columns with the same name."""
    lake.put([{"id": 1, "_prefect_extracted_at": "x"}])

    with pytest.raises(ValueError, match="reserved"):
        await lake.dump(columns={"add_timestamp": True})


async def test_normalize_values_cleans_the_text_columns(lake):
    """Trim the text inside the dump."""
    await lake.dump(chunks=DAYS, columns={"normalize_values": True})

    assert next(row[1] for row in lake.rows()) == "Ana"


async def test_a_new_config_loads_every_chunk_again(lake):
    """Treat the chunks as not loaded when what is written changes."""
    first = await lake.dump(chunks=DAYS)

    second = await lake.dump(chunks=DAYS, columns={"normalize_values": True})

    assert second == first + 3 + NULL_CHUNK


async def test_append_refuses_a_change_of_columns_and_overwrite_applies_it(lake):
    """Keep one schema in append, and replace it when the dump is rebuilt in overwrite."""
    await lake.dump(chunks=DAYS)

    with pytest.raises(SchemaMismatchError):
        await lake.dump(chunks=DAYS, select=["id", "dia"])

    await lake.dump("overwrite", chunks=DAYS, select=["id", "dia"])

    table = DeltaTable(lake.uri).to_pyarrow_table()
    assert table.column_names == ["id", "dia", "batch"]
    assert table.num_rows == len(ROWS)


async def test_overwrite_writes_every_chunk_again_and_sweeps_the_old_ones(lake):
    """Rewrite the same data in overwrite mode, and delete the chunks outside the range."""
    await lake.dump(chunks=DAYS)
    before = lake.writes()

    await lake.dump("overwrite", chunks={**DAYS, "begin": "2024-03-02"})

    assert lake.writes() == before + 2 + NULL_CHUNK
    assert [row[0] for row in lake.rows()] == [3, 4]


async def test_overwrite_ignores_lookback_and_rebuilds_every_chunk(lake):
    """Rewrite all the chunks in overwrite mode, even when lookback names only the last one."""
    await lake.dump(chunks=DAYS)
    before = lake.writes()

    await lake.dump("overwrite", chunks={**DAYS, "lookback": 1})

    assert lake.writes() == before + 3 + NULL_CHUNK


async def test_an_earlier_chunk_never_loaded_is_loaded_even_with_lookback(lake):
    """Let lookback limit the re-checks only, and not the first load of an older chunk."""
    await lake.dump(chunks={**DAYS, "begin": "2024-03-02"})

    await lake.dump(chunks={**DAYS, "lookback": 1})

    assert [row[0] for row in lake.rows()] == [1, 2, 3, 4]


async def test_a_new_filter_reaches_the_chunks_that_lookback_does_not_recheck(lake):
    """Treat every chunk as not loaded after a new filter, so no old chunk keeps its rows."""
    await lake.dump(chunks=DAYS)

    await lake.dump(chunks={**DAYS, "lookback": 1}, filters=["valor > 15"])

    assert [row[0] for row in lake.rows()] == [2, 3, 4]


async def test_a_dump_that_writes_leaves_a_checkpoint_for_bigquery(lake):
    """Write a checkpoint after the chunks, so a query does not replay a long log."""
    await lake.dump(chunks=DAYS)

    log = Path(lake.uri, "_delta_log")
    assert any("checkpoint" in entry.name for entry in log.iterdir())


async def test_lookback_skips_the_chunks_that_it_does_not_recheck(lake):
    """Do not read an old chunk again, but always load a chunk that is new."""
    await lake.dump(chunks=DAYS)
    lake.put([{**ROWS[0], "valor": 11.0}, *ROWS[1:], {**ROWS[3], "id": 5, "dia": date(2024, 3, 4)}])
    wider = {**DAYS, "end": "2024-03-04", "lookback": 1}

    await lake.dump(chunks=wider)

    by_id = {row[0]: row for row in lake.rows()}
    assert by_id[1][3] == 10.0
    assert 5 in by_id


async def test_without_batches_the_whole_source_is_one_chunk(lake):
    """Replace the whole table when the data changes, and skip it when it does not."""
    first = await lake.dump()
    assert await lake.dump() == first

    lake.put(ROWS[:2])
    await lake.dump()

    assert [row[0] for row in lake.rows()] == [1, 2]


@pytest.mark.parametrize(
    ("options", "message"),
    [
        pytest.param({"chunks": {**DAYS, "column": "nada"}}, "must exist", id="a_chunk_column_that_is_missing"),
        pytest.param({"filters": ["id = 1; DROP TABLE t"]}, "one predicate", id="a_second_statement_in_a_filter"),
        pytest.param({"filters": [" "]}, "is empty", id="an_empty_filter"),
        pytest.param({"select": ["nada"]}, "Unknown columns", id="a_missing_column_to_select"),
    ],
)
async def test_an_option_that_does_not_fit_fails_before_any_write(lake, options, message):
    """Raise before the table is touched."""
    with pytest.raises(ValueError, match=message):
        await lake.dump(**options)

    assert not DeltaTable.is_deltatable(lake.uri)


async def test_a_delta_table_is_synced_over_its_prefix_with_the_connection(bigquery):
    """Create the dataset, and point the BigLake table at the Delta folder."""
    await publish(DESTINATION, "p", "p.us.conn")

    assert bigquery.calls == [
        ("connect", "p"),
        ("dataset", "p.ds_staging"),
        ("delta", "p.ds_staging.tb", "gcs://b/staging/ds/tb", "p.us.conn"),
    ]


@pytest.mark.usefixtures("bigquery")
async def test_a_delta_table_without_a_connection_is_refused():
    """Raise, because BigQuery reads a Delta table only through a BigLake connection."""
    with pytest.raises(ValueError, match="BigLake connection"):
        await publish(DESTINATION, "p", None)


@pytest.mark.parametrize(
    ("db_type", "read", "batched", "expected"),
    [
        pytest.param("postgres", {"table": "public.orders"}, True, TableOrigin("public.orders"), id="a_table"),
        pytest.param("oracle", {"table": "APP.ORDERS"}, True, TableOrigin("APP.ORDERS"), id="a_table_of_oracle"),
        pytest.param("postgres", {"query": "SELECT 1"}, False, Query("SELECT 1", "postgres"), id="a_native_query"),
        pytest.param("bigquery", {"query": "SELECT 1"}, False, Query("SELECT 1", "bigquery"), id="a_bigquery_query"),
        pytest.param(
            "oracle",
            {"query": "SELECT 1 FROM dual"},
            True,
            Query("SELECT 1 FROM dual", "oracle"),
            id="a_query_of_oracle_is_cut",
        ),
        pytest.param("couchdb", {"table": "db"}, True, TableOrigin("db"), id="a_couchdb_database"),
        pytest.param("couchdb", {"query": "{}"}, False, Query("{}", "couchdb"), id="a_couchdb_selector"),
        pytest.param(
            "couchdb",
            {"table": "db", "query": "{}"},
            False,
            Query("{}", "couchdb", "db"),
            id="a_selector_of_a_database",
        ),
        pytest.param("couchdb", {"since": "0"}, False, Changes("0"), id="couchdb_changes"),
        pytest.param(
            "couchdb", {"table": "db", "since": "now"}, False, Changes("now", "db"), id="changes_of_a_database"
        ),
        pytest.param("mongodb", {"table": "pessoas"}, True, TableOrigin("pessoas"), id="a_mongodb_collection"),
        pytest.param(
            "mongodb",
            {"table": "p", "query": '{"a": 1}'},
            False,
            Query('{"a": 1}', "mongodb", "p"),
            id="a_mongodb_selector",
        ),
    ],
)
def test_a_read_names_the_origin_that_the_database_reads(db_type, read, batched, expected):
    """Give a table, a query or the changes of a database, as the type of database reads."""
    assert read_request(db_type, read, batched) == expected


@pytest.mark.parametrize(
    ("db_type", "read", "batched", "message"),
    [
        pytest.param("postgres", {}, False, "exactly one", id="neither"),
        pytest.param("postgres", {"table": "a.b", "query": "SELECT 1"}, False, "exactly one", id="both"),
        pytest.param("postgres", {"query": "SELECT 1"}, True, "one chunk", id="chunks_over_a_query"),
        pytest.param("couchdb", {}, False, "Give a table", id="a_database_with_nothing"),
        pytest.param("couchdb", {"query": "{}", "since": "0"}, False, "not both", id="query_and_since"),
        pytest.param("mongodb", {"table": "c", "since": "0"}, False, "only CouchDB", id="mongodb_has_no_changes"),
    ],
)
def test_a_read_that_does_not_fit_is_refused(db_type, read, batched, message):
    """Raise before any connection, so a typo never reaches the database."""
    with pytest.raises(ValueError, match=message):
        read_request(db_type, read, batched)


@pytest.fixture
def batches_in_a_bucket(store, lake, monkeypatch):
    """Return a function that copies the lake in batches to an in-memory bucket."""
    monkeypatch.setattr(utils, "create_store", lambda *_: store)
    values = Files(f"{lake.source}/*.parquet", "parquet")

    async def run(dump_mode: DumpMode, batch_size: int) -> None:
        """Copy the four sample rows."""
        await dump_in_batches(FilesSource(origin=values), DESTINATION, dump_mode, batch_size)

    return run


async def test_a_source_is_copied_in_batches_of_the_given_size(store, batches_in_a_bucket, stored_rows):
    """Write one Parquet object for each batch of rows, and lose no row."""
    await batches_in_a_bucket("append", 3)

    keys = await gcs.list_keys(store, DESTINATION.prefix)
    assert len(keys) == 2
    assert await stored_rows(store, DESTINATION.prefix) == [1, 2, 3, 4]


async def test_append_keeps_the_old_objects_and_overwrite_deletes_them(store, batches_in_a_bucket, put):
    """Add objects in append mode, and start from an empty prefix in overwrite mode."""
    await put(store, f"{DESTINATION.prefix}/old.parquet")

    await batches_in_a_bucket("append", 10)
    assert f"{DESTINATION.prefix}/old.parquet" in await gcs.list_keys(store, DESTINATION.prefix)

    await batches_in_a_bucket("overwrite", 10)
    keys = await gcs.list_keys(store, DESTINATION.prefix)
    assert f"{DESTINATION.prefix}/old.parquet" not in keys
    assert len(keys) == 1


@pytest.mark.parametrize(
    ("options", "fails"),
    [
        pytest.param(None, False, id="no_options"),
        pytest.param({"select": ["a"]}, False, id="options_without_batches"),
        pytest.param({"chunks": DAYS}, True, id="batches_over_a_source_read_whole"),
    ],
)
def test_a_source_read_whole_is_never_cut_into_chunks(options, fails):
    """Refuse chunks over a source that DuckDB cannot filter, so it is not read many times."""
    if fails:
        with pytest.raises(ValueError, match="one chunk"):
            without_chunks(options, "A layer")
    else:
        assert without_chunks(options, "A layer") == (options or {})


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(("overwrite", False, True), id="overwrite_clears_the_files_of_an_older_layout"),
        pytest.param(("overwrite", True, False), id="overwrite_never_deletes_a_delta_table"),
        pytest.param(("append", False, False), id="append_never_deletes_anything"),
    ],
)
async def test_the_files_of_an_older_layout_go_before_an_overwrite_builds_a_delta_table(store, monkeypatch, case, put):
    """Delete the old Hive files only when an overwrite meets no Delta table."""
    dump_mode, is_delta, cleared = case
    monkeypatch.setattr(utils, "create_store", lambda *_: store)
    monkeypatch.setattr(DeltaTable, "is_deltatable", staticmethod(lambda *_: is_delta))
    key = f"{DESTINATION.prefix}/old.parquet"
    await put(store, key)

    await clear_legacy(DESTINATION, dump_mode)

    assert (key not in await gcs.list_keys(store, DESTINATION.prefix)) is cleared


async def test_a_delta_load_clears_the_older_layout_before_it_writes(monkeypatch):
    """Clear first, then dump, so the new table never sits beside old Hive files."""
    order: list[str] = []

    async def fake_clear(*args: object) -> None:
        """Record the clear, with its dump mode."""
        order.append(f"clear:{args[1]}")

    async def fake_dump(*args: object) -> None:
        """Record the dump, with its URI."""
        order.append(f"dump:{args[1]}")

    monkeypatch.setattr(utils, "clear_legacy", fake_clear)
    monkeypatch.setattr(utils, "dump_to_delta", fake_dump)

    await load_to_delta(FilesSource(origin=Files("x.parquet", "parquet")), DESTINATION, "overwrite", None)

    assert order == ["clear:overwrite", "dump:gcs://b/staging/ds/tb"]


async def test_a_number_width_cuts_an_integer_column_into_runs_of_ids(lake):
    """Chunk by runs of ids, and keep every row."""
    commits = await lake.dump(chunks={"column": "id", "width": 2, "begin": 1, "end": 4})

    assert commits == 2 + NULL_CHUNK
    assert [row[0] for row in lake.rows()] == [1, 2, 3, 4]


async def test_a_date_width_also_cuts_a_timestamp_column(lake, naive):
    """Chunk by day over a timestamp, and keep the rows of a day whatever the time."""
    lake.put(
        [
            {"id": 1, "dia": naive(2024, 3, 1, 9), "nome": "a", "valor": 1.0},
            {"id": 2, "dia": naive(2024, 3, 1, 23, 59), "nome": "b", "valor": 2.0},
            {"id": 3, "dia": naive(2024, 3, 2, 0, 1), "nome": "c", "valor": 3.0},
        ]
    )

    commits = await lake.dump(chunks=DAYS)

    assert commits == 3 + NULL_CHUNK
    assert [row[0] for row in lake.rows()] == [1, 2, 3]


@pytest.mark.parametrize(
    ("chunks", "message"),
    [
        pytest.param(
            {"column": "nome", "width": "day", "begin": "2024-03-01"},
            "a date or a timestamp",
            id="a_date_width_on_text",
        ),
        pytest.param(
            {"column": "valor", "width": "day", "begin": "2024-03-01"},
            "a date or a timestamp",
            id="a_date_width_on_a_float",
        ),
        pytest.param({"column": "dia", "width": 10, "begin": 0, "end": 5}, "an integer", id="a_number_width_on_a_date"),
        pytest.param({"column": "nome", "width": 10, "begin": 0, "end": 5}, "an integer", id="a_number_width_on_text"),
    ],
)
async def test_the_chunk_column_must_have_the_type_that_the_chunks_need(lake, chunks, message):
    """Refuse a column of another type before any write, as a database would fail on the compare."""
    with pytest.raises(ValueError, match=message):
        await lake.dump(chunks=chunks)

    assert not DeltaTable.is_deltatable(lake.uri)


READABLE = {"timestampNtz", "columnMapping", "deletionVectors"}
"""The reader features that BigQuery supports, with reader version 3 as the most it reads."""
IDENTIFIER = compile_pattern(r"[A-Za-z_][A-Za-z0-9_]*")


def delta_log(uri: str) -> tuple[dict[str, object], dict[str, str]]:
    """Return the last protocol and the schema, name to type, that the log of a table records."""
    protocol: dict[str, object] = {}
    schema: dict[str, str] = {}
    for entry in sorted(Path(uri, "_delta_log").glob("*.json")):
        for line in entry.read_text().splitlines():
            action = loads(line)
            if "protocol" in action:
                protocol = action["protocol"]
            if "metaData" in action:
                fields = loads(action["metaData"]["schemaString"])["fields"]
                schema = {field["name"]: field["type"] for field in fields}
    return protocol, schema


def assert_bigquery_can_read(uri: str) -> None:
    """Check the limits that Google lists for a Delta table of BigLake, on the log itself."""
    protocol, schema = delta_log(uri)
    features = set(cast("list[str]", protocol.get("readerFeatures", [])))

    assert cast("int", protocol["minReaderVersion"]) <= 3
    assert features <= READABLE
    assert not Path(uri, "_delta_log", "_sidecars").exists()
    assert all(IDENTIFIER.fullmatch(name) for name in schema)


async def after_first_load(lake: Lake) -> None:
    """Load the three days."""
    await lake.dump(chunks=DAYS)


async def after_a_replaced_chunk(lake: Lake) -> None:
    """Change one row, so one day is replaced."""
    await lake.dump(chunks=DAYS)
    lake.put([*ROWS[:2], {**ROWS[2], "valor": 99.0}, ROWS[3]])
    await lake.dump(chunks=DAYS)


async def after_an_empty_chunk(lake: Lake) -> None:
    """Delete a whole day, so its rows are removed."""
    await lake.dump(chunks=DAYS)
    lake.put(ROWS[:3])
    await lake.dump(chunks=DAYS)


async def after_an_overwrite_with_other_columns(lake: Lake) -> None:
    """Rebuild with fewer columns, which replaces the schema."""
    await lake.dump(chunks=DAYS)
    await lake.dump("overwrite", chunks=DAYS, select=["id", "dia"])


@pytest.mark.extensions
@pytest.mark.parametrize(
    "scenario",
    [
        pytest.param(after_first_load, id="after_the_first_load"),
        pytest.param(after_a_replaced_chunk, id="after_a_replaced_chunk"),
        pytest.param(after_an_empty_chunk, id="after_an_empty_chunk"),
        pytest.param(after_an_overwrite_with_other_columns, id="after_an_overwrite_with_other_columns"),
    ],
)
async def test_a_reader_that_is_not_ours_sees_what_we_wrote_and_the_log_fits_bigquery(lake, delta_reader, scenario):
    """Read the table with DuckDB after each kind of write, as BigQuery reads the log, and check its limits."""
    await scenario(lake)

    names = [field.name for field in DeltaTable(lake.uri).schema().fields if field.name != "batch"]
    columns = ", ".join(names)
    read = delta_reader.execute(f"SELECT {columns} FROM delta_scan('{lake.uri}') ORDER BY 1").fetchall()
    ours = DeltaTable(lake.uri).to_pyarrow_table().select(names).sort_by("id").to_pylist()
    assert [tuple(row.values()) for row in ours] == read
    assert_bigquery_can_read(lake.uri)


async def test_the_types_of_the_pipelines_get_the_delta_types_that_bigquery_maps(tmp_path):
    """Pin the Delta type of each column, which BigQuery reads as INTEGER, NUMERIC, DATETIME and so on."""
    instant = datetime(2024, 3, 1, 10, tzinfo=UTC)
    dataframe = arrow_table(
        {
            "id": [1],
            "texto": ["a"],
            "numero": [1.5],
            "decimal": arrow_array([Decimal("1.25")], decimal128(10, 2)),
            "flag": [True],
            "dia": arrow_array([date(2024, 3, 1)]),
            "instante": arrow_array([instant.replace(tzinfo=None)], arrow_timestamp("us")),
            "instante_utc": arrow_array([instant], arrow_timestamp("us", "UTC")),
        }
    )
    write_table(dataframe, tmp_path / "types.parquet")
    uri = str(tmp_path / "table")

    await dump_to_delta(FilesSource(origin=Files(str(tmp_path / "types.parquet"), "parquet")), uri, "append", {})

    assert delta_log(uri)[1] == {
        "id": "long",
        "texto": "string",
        "numero": "double",
        "decimal": "decimal(10,2)",
        "flag": "boolean",
        "dia": "date",
        "instante": "timestamp_ntz",
        "instante_utc": "timestamp",
        "batch": "string",
    }
    assert_bigquery_can_read(uri)


@pytest.mark.parametrize(
    ("data", "key", "expected"),
    [
        pytest.param([{"id": 1}], None, [{"id": 1}], id="the_response_is_the_list"),
        pytest.param({"items": [{"id": 1}]}, "items", [{"id": 1}], id="a_key"),
        pytest.param({"data": {"items": [{"id": 1}]}}, "data.items", [{"id": 1}], id="a_dotted_key"),
        pytest.param({"items": []}, "items", [], id="an_empty_list"),
    ],
)
def test_the_records_are_the_list_at_the_key(data, key, expected):
    """Pick the list out of the response."""
    assert extract_records(data, key) == expected


@pytest.mark.parametrize(
    ("data", "key"),
    [
        pytest.param({"items": []}, "other", id="a_missing_key"),
        pytest.param({"items": {"id": 1}}, "items", id="an_object_instead_of_a_list"),
        pytest.param([1, 2], None, id="a_list_of_numbers"),
        pytest.param({"items": []}, None, id="an_object_with_no_key"),
        pytest.param({"a": 1}, "a.b", id="a_key_below_a_number"),
    ],
)
def test_a_response_with_no_list_of_objects_is_refused(data, key):
    """Raise, so a change in the API fails the run instead of loading nothing."""
    with pytest.raises(ValueError, match="No list of objects"):
        extract_records(data, key)


async def test_json_lines_load_as_rows_and_nested_values_stay_nested(tmp_path):
    """Write the records, and read them back with DuckDB as typed columns."""
    folder = write_json_lines(
        [{"id": 1, "nome": "Ana", "tags": ["a", "b"]}, {"id": 2, "nome": "Bia", "tags": []}], tmp_path
    )
    uri = str(tmp_path / "table")

    await dump_to_delta(
        FilesSource(origin=Files(f"{folder}/**/*.json", "json")),
        uri,
        "append",
        {"capture": datetime(2024, 3, 10, 9, tzinfo=UTC)},
    )

    table = DeltaTable(uri).to_pyarrow_table().sort_by("id").to_pylist()
    assert [(row["id"], row["nome"], row["tags"]) for row in table] == [(1, "Ana", ["a", "b"]), (2, "Bia", [])]


async def test_a_query_is_read_into_one_dataframe(lake):
    """Return every row of the source in one Polars dataframe."""
    dataframe = await read_dataframe(FilesSource(origin=Files(f"{lake.source}/*.parquet", "parquet")))

    assert dataframe["id"].to_list() == [1, 2, 3, 4]


def test_the_capture_time_is_the_scheduled_start_in_sao_paulo():
    """Give a time with the offset of São Paulo, which is not UTC."""
    assert capture_time().utcoffset() == timedelta(hours=-3)


@pytest.mark.parametrize(
    ("crs", "expected"),
    [
        pytest.param("EPSG:31983", "31983", id="with_the_prefix"),
        pytest.param("epsg:3857", "3857", id="in_lower_case"),
        pytest.param("4326", "4326", id="the_number_only"),
    ],
)
def test_an_epsg_code_is_its_number(crs, expected):
    """Give the number that ArcGIS takes as `outSR`."""
    assert epsg_code(crs) == expected


@pytest.mark.parametrize("crs", ["WGS84", "EPSG:", "EPSG:31983; DROP", ""])
def test_a_text_that_is_not_an_epsg_code_is_refused(crs):
    """Raise before the number goes into a URL."""
    with pytest.raises(ValueError, match="EPSG"):
        epsg_code(crs)


@pytest.mark.parametrize("workers", [2, 4])
async def test_chunks_read_together_land_in_the_table_as_chunks_read_one_by_one(lake, tmp_path, workers):
    """Write the same rows and the same chunks, whether one worker reads or several."""
    origin = Files(f"{lake.source}/*.parquet", "parquet")
    together = str(tmp_path / "together")

    await dump_to_delta(ConcurrentFilesSource(workers, origin), together, "append", {"chunks": DAYS})
    await lake.dump(chunks=DAYS)

    def chunks(uri: str) -> list[tuple[object, ...]]:
        """Return the rows of a table with their chunk, in a fixed order."""
        table = DeltaTable(uri).to_pyarrow_table().sort_by("id")
        return [(row["id"], row[BATCH_COLUMN]) for row in table.to_pylist()]

    assert chunks(together) == chunks(lake.uri)
    assert len(chunks(together)) == len(ROWS)


@pytest.mark.parametrize("schema", [ItemModel, ItemData, ItemDict], ids=["model", "dataclass", "typeddict"])
def test_records_that_fit_a_schema_pass(schema):
    """Accept a Pydantic model, a dataclass and a `TypedDict`."""
    validate_records([{"id": 1, "nome": "Ana"}, {"id": 2, "nome": "Bia"}], schema)


@pytest.mark.parametrize("schema", [ItemModel, ItemData, ItemDict], ids=["model", "dataclass", "typeddict"])
def test_a_record_that_does_not_fit_names_its_index_and_its_field(schema):
    """Raise at the first bad record, and name where it is."""
    records = [{"id": 1, "nome": "Ana"}, {"id": "dois", "nome": "Bia"}]

    with pytest.raises(ValueError, match=r"record 1: id: "):
        validate_records(records, schema)


def test_the_error_of_a_bad_record_never_holds_its_value():
    """Keep the data out of the message, so a log or a Discord alert never leaks it."""
    secret = "valor-secreto-123"

    with pytest.raises(ValueError, match="record 0") as error:
        validate_records([{"id": secret, "nome": "Ana"}], ItemModel)

    assert secret not in str(error.value)


def test_a_missing_field_is_named():
    """Name a field that the record lacks."""
    with pytest.raises(ValueError, match=r"record 0: nome: Field required"):
        validate_records([{"id": 1}], ItemModel)


def test_records_are_not_changed_by_the_check():
    """Leave the records as the API sent them, with no coercion."""
    records = [{"id": "1", "nome": "Ana"}]

    validate_records(records, ItemModel)

    assert records == [{"id": "1", "nome": "Ana"}]


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        pytest.param("MODEL", ItemModel, id="a_model"),
        pytest.param("DATA", ItemData, id="a_dataclass"),
        pytest.param("DICT", ItemDict, id="a_typeddict"),
    ],
)
@pytest.mark.usefixtures("schemas")
def test_a_records_schema_is_found_by_its_path(name, expected):
    """Return the type that `module:name` names."""
    assert load_records_schema(f"schemas_of_the_test:{name}") is expected


@pytest.mark.parametrize(
    ("path", "message"),
    [
        pytest.param("no_colon", "module:name", id="a_path_without_a_colon"),
        pytest.param("missing_module_of_the_test:X", "cannot be imported", id="a_module_that_does_not_exist"),
        pytest.param("schemas_of_the_test:MISSING", "is not a type", id="a_name_that_does_not_exist"),
        pytest.param("schemas_of_the_test:NOT_A_TYPE", "is not a type", id="a_name_of_another_kind"),
    ],
)
@pytest.mark.usefixtures("schemas")
def test_a_records_schema_that_cannot_be_found_is_refused(path, message):
    """Raise `ValueError` that says what is wrong with the path."""
    with pytest.raises(ValueError, match=message):
        load_records_schema(path)


async def test_discovery_keeps_only_the_folders_that_have_a_delta_log(store, put):
    """Find the tables of the staging folder and skip a folder of loose files."""
    await put(
        store,
        "staging/ds/tb/_delta_log/00000000000000000000.json",
        "staging/ds/tb/batch=all/a.parquet",
        "staging/ds/old/part-1.parquet",
        "staging/other/tb2/_delta_log/00000000000000000000.json",
        "elsewhere/ds/tb3/_delta_log/00000000000000000000.json",
    )

    found = await discover_tables(store, "b")

    assert found == [Destination("b", "ds", "tb"), Destination("b", "other", "tb2")]


def test_the_maintenance_message_lists_the_failures_then_the_layout_problems(report):
    """Put the failed tables first, and count the files that the run merged."""
    done = [
        Maintenance(table="ds.a", before=report(10), after=report(2), problems=[]),
        Maintenance(table="ds.b", before=report(5), after=report(1), problems=["all next to chunk keys"]),
    ]

    lines = summarize_maintenance(done, {"ds.c": "no connection"}).splitlines()

    assert lines == [
        "**Delta maintenance**: 2 ok, 1 failed",
        ":red_circle: `ds.c`: no connection",
        ":warning: `ds.b`: all next to chunk keys",
        "Files: 15 to 3",
    ]


def test_the_maintenance_message_stays_within_the_limit_of_discord():
    """Cut a long list of failures to what Discord accepts."""
    failed = {f"ds.t{number}": "x" * 100 for number in range(100)}

    assert len(summarize_maintenance([], failed)) == DISCORD_LIMIT


@pytest.mark.parametrize(
    ("outcome", "expected"),
    [
        pytest.param(
            DbtOutcome(success=True, nodes=[]),
            [
                ":white_check_mark: **[queries-x]** `dbt build` finished without errors",
                "- Target: `prod`",
                "- Select: `tag:daily`",
            ],
            id="a_clean_run_lists_no_node",
        ),
        pytest.param(
            DbtOutcome(
                success=False,
                nodes=[DbtNode(name="a", status="fail", message="m1"), DbtNode(name="b", status="warn", message="m2")],
            ),
            [
                ":x: **[queries-x]** `dbt build` finished with errors",
                "- Target: `prod`",
                "- Select: `tag:daily`",
                ":red_circle: `a` fail: m1",
                ":warning: `b` warn: m2",
            ],
            id="the_nodes_that_went_wrong_are_listed",
        ),
    ],
)
def test_the_dbt_message_names_the_repository_and_never_its_url(outcome, expected):
    """Show the name of the repository and the nodes, with no URL (it can hold a token)."""
    request = DbtRun(
        repository="https://TOKEN@github.com/org/queries-x.git", command="build", target="prod", select="tag:daily"
    )

    message = summarize_dbt(request, outcome)

    assert message.splitlines() == expected
    assert "TOKEN" not in message


def test_the_dbt_message_stays_within_the_limit_of_discord():
    """Cut a long list of nodes to what Discord accepts."""
    nodes = [DbtNode(name=f"n{number}", status="error", message="x" * 100) for number in range(100)]

    message = summarize_dbt(DbtRun(repository="https://x/y.git"), DbtOutcome(success=False, nodes=nodes))

    assert len(message) == DISCORD_LIMIT


def test_only_the_nodes_that_failed_or_warned_are_read_from_a_dbt_result(dbt_node):
    """Skip the nodes that passed, and take a node name or a plain unique id."""
    result = SimpleNamespace(
        result=SimpleNamespace(
            results=[
                dbt_node("ok", "success"),
                dbt_node("bad", "error", "boom"),
                dbt_node("late", "runtime error"),
                SimpleNamespace(status="fail", message=None, unique_id="operation.x.y"),
            ]
        )
    )

    assert read_nodes(result) == [
        DbtNode(name="bad", status="error", message="boom"),
        DbtNode(name="late", status="runtime error", message=""),
        DbtNode(name="operation.x.y", status="fail", message=""),
    ]


def test_a_dbt_result_with_no_nodes_has_none_to_read():
    """Give an empty list for a command such as `debug`."""
    assert read_nodes(SimpleNamespace(success=True)) == []
