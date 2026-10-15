from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from io import BytesIO
from json import loads
from pathlib import Path
from re import compile as compile_pattern
from typing import Unpack, cast

import pytest
from deltalake import DeltaTable
from deltalake.exceptions import SchemaMismatchError
from duckdb import DuckDBPyConnection, Error
from duckdb import connect as duckdb_connect
from httpx2 import ConnectError, ConnectTimeout, HTTPStatusError, Request, Response
from polars import read_parquet
from psycopg.sql import Composable
from pyarrow import Table, decimal128
from pyarrow import array as arrow_array
from pyarrow import table as arrow_table
from pyarrow import timestamp as arrow_timestamp
from pyarrow.parquet import write_table
from toolkit import gcs
from toolkit.executor.sources.backends.files import FilesSource
from toolkit.types import Batches, TemplateValue

from utils import utils
from utils.constants import DISCORD_LIMIT
from utils.types import DumpMode, DumpOptions
from utils.utils import (
    Destination,
    capture_time,
    clear_legacy,
    clear_prefix,
    dump_in_batches,
    dump_to_delta,
    epsg_code,
    extract_records,
    failure_message,
    files_glob,
    fill_secrets,
    is_transient,
    load_to_delta,
    publish,
    read_frame,
    read_values,
    source_values,
    without_batches,
    write_json_lines,
)

DESTINATION = Destination("b", "ds", "tb")


async def put(store, *keys: str) -> None:
    """Store a small object under each key."""
    for key in keys:
        await store.put_async(key, b"old")


def test_a_destination_names_the_files_and_the_external_table():
    """Name the folder, the URI, the dataset and the table of one BigQuery table."""
    assert DESTINATION.prefix == "staging/ds/tb"
    assert DESTINATION.uri == "gcs://b/staging/ds/tb"
    assert DESTINATION.dataset("p") == "p.ds_staging"
    assert DESTINATION.table("p") == "p.ds_staging.tb"


async def test_regression_clearing_a_table_leaves_a_similar_name_alone(store):
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
NULL_WINDOW = 1  # a dump with `batches` also has the window of the rows with no value
DAYS: Batches = {"column": "dia", "width": "day", "begin": "2024-03-01", "end": "2024-03-03"}


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
        values = source_values("parquet", f"{self.source}/*.parquet")
        await dump_to_delta(FilesSource(), values, self.uri, dump_mode, options)
        return DeltaTable(self.uri).version() + 1

    def writes(self) -> int:
        """Return how many commits wrote data."""
        return sum(1 for entry in DeltaTable(self.uri).history() if entry.get("operation") == "WRITE")

    def rows(self) -> list[tuple[object, ...]]:
        """Return the rows of the table, sorted by id, without the window column."""
        table = DeltaTable(self.uri).to_pyarrow_table().drop(["batch"])
        return sorted((tuple(row.values()) for row in table.to_pylist()), key=lambda r: r[0])


@pytest.fixture
def lake(tmp_path: Path) -> Lake:
    """Return a lake with the four sample rows in the source."""
    lake = Lake(tmp_path)
    lake.put(ROWS)
    return lake


async def test_the_first_dump_loads_every_window(lake):
    """Write each day as its own window, and normalize the names of the columns."""
    commits = await lake.dump(batches=DAYS)

    assert commits == 3 + NULL_WINDOW
    assert [row[0] for row in lake.rows()] == [1, 2, 3, 4]
    assert sorted(cast("list[str]", DeltaTable(lake.uri).to_pyarrow_table().column("batch").to_pylist())) == [
        "2024-03-01",
        "2024-03-01",
        "2024-03-02",
        "2024-03-03",
    ]


async def test_the_same_data_is_never_written_again(lake):
    """Skip every window whose fingerprint is in the ledger, so the table gets no commit."""
    first = await lake.dump(batches=DAYS)

    second = await lake.dump(batches=DAYS)

    assert second == first


async def test_a_changed_window_is_replaced_and_the_others_are_left(lake):
    """Commit once for the one day that changed, and keep the other days as they are."""
    first = await lake.dump(batches=DAYS)
    lake.put([*ROWS[:2], {**ROWS[2], "valor": 99.0}, ROWS[3]])

    second = await lake.dump(batches=DAYS)

    assert second == first + 1
    assert next(row for row in lake.rows() if row[0] == 3)[3] == 99.0
    assert len(lake.rows()) == 4


async def test_a_window_that_became_empty_loses_its_rows(lake):
    """Expire the rows of a day that has none in the source now."""
    await lake.dump(batches=DAYS)
    lake.put(ROWS[:3])

    await lake.dump(batches=DAYS)

    assert [row[0] for row in lake.rows()] == [1, 2, 3]


async def test_regression_rows_with_no_value_in_the_batch_column_are_loaded(lake):
    """Regression: a real table had NULL dates, and the windows left them out silently."""
    lake.put([*ROWS, {"id": 5, "nome": "Eli", "dia": None, "valor": 50.0}])

    await lake.dump(batches=DAYS)

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

    await lake.dump(batches={"column": "instante", "width": "day", "begin": "2024-02-28", "end": "2024-03-03"})

    table = DeltaTable(lake.uri).to_pyarrow_table().to_pylist()
    assert {row["id"]: row["batch"] for row in table} == {1: "2024-02-29", 2: "2024-03-01", 3: "2024-03-01"}


async def test_the_rows_with_no_value_change_with_the_source(lake):
    """Replace the NULL window like any other, so a removed row goes away."""
    lake.put([*ROWS, {"id": 5, "nome": "Eli", "dia": None, "valor": 50.0}])
    await lake.dump(batches=DAYS)

    lake.put(ROWS)
    await lake.dump(batches=DAYS)

    assert [row[0] for row in lake.rows()] == [1, 2, 3, 4]


async def test_filters_and_select_narrow_the_rows_and_the_columns(lake):
    """Keep the rows that pass the filters, and only the columns that were asked for."""
    await lake.dump(
        batches=DAYS,
        filters=[{"column": "valor", "op": ">", "value": 15}],
        select=["id", "dia"],
    )

    table = DeltaTable(lake.uri).to_pyarrow_table()
    assert table.column_names == ["id", "dia", "batch"]
    assert sorted(cast("list[int]", table.column("id").to_pylist())) == [2, 3, 4]


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
        FilesSource(),
        source_values("parquet", f"{elsewhere}/*.parquet"),
        lake.uri,
        "append",
        {"capture": FIRST_RUN},
    )

    assert [row[0] for row in lake.rows()] == [1, 2, 3, 4]


async def test_a_capture_is_in_the_window_of_its_day(lake):
    """Name the window after the day of the capture, so a day is a partition."""
    await lake.dump(capture=FIRST_RUN)
    await lake.dump(capture=SECOND_RUN)
    await lake.dump(capture=NEXT_DAY_RUN)

    batch = {row["batch"] for row in DeltaTable(lake.uri).to_pyarrow_table().to_pylist()}
    assert batch == {"2024-03-10", "2024-03-11"}


@pytest.mark.parametrize(
    ("dump_mode", "options"),
    [
        pytest.param("overwrite", {}, id="overwrite"),
        pytest.param("append", {"batches": DAYS}, id="batches"),
    ],
)
async def test_a_capture_refuses_what_would_replace_the_earlier_ones(lake, dump_mode, options):
    """Raise before any write, because a capture only adds."""
    with pytest.raises(ValueError, match="capture"):
        await lake.dump(dump_mode, capture=FIRST_RUN, **options)

    assert not DeltaTable.is_deltatable(lake.uri)


def stamps(lake) -> dict[int, datetime]:
    """Return the extraction time of each id."""
    table = DeltaTable(lake.uri).to_pyarrow_table()
    ids, times = table.column("id").to_pylist(), table.column("_prefect_extracted_at").to_pylist()
    return cast("dict[int, datetime]", dict(zip(ids, times, strict=True)))


async def test_the_extraction_time_is_added_and_only_a_changed_window_gets_a_new_one(lake):
    """Keep the time of a window that did not change, and stamp the one that was replaced."""
    await lake.dump(batches=DAYS, columns={"add_timestamp": True})
    first = stamps(lake)

    lake.put([ROWS[0], ROWS[1], {**ROWS[2], "valor": 99.0}, ROWS[3]])
    await lake.dump(batches=DAYS, columns={"add_timestamp": True})
    second = stamps(lake)

    assert (second[1], second[2], second[4]) == (first[1], first[2], first[4])
    assert second[3] > first[3]


async def test_the_extraction_time_does_not_make_a_window_look_changed(lake):
    """Write no commit on a rerun, because the time is not part of the fingerprint."""
    commits = await lake.dump(batches=DAYS, columns={"add_timestamp": True})

    assert await lake.dump(batches=DAYS, columns={"add_timestamp": True}) == commits


async def test_a_source_column_with_the_name_of_the_time_is_refused(lake):
    """Raise, because the dump would write two columns with the same name."""
    lake.put([{"id": 1, "_prefect_extracted_at": "x"}])

    with pytest.raises(ValueError, match="reserved"):
        await lake.dump(columns={"add_timestamp": True})


async def test_normalize_values_cleans_the_text_columns(lake):
    """Trim the text inside the dump."""
    await lake.dump(batches=DAYS, columns={"normalize_values": True})

    assert next(row[1] for row in lake.rows()) == "Ana"


async def test_a_new_config_loads_every_window_again(lake):
    """Treat the windows as not loaded when what is written changes."""
    first = await lake.dump(batches=DAYS)

    second = await lake.dump(batches=DAYS, columns={"normalize_values": True})

    assert second == first + 3 + NULL_WINDOW


async def test_append_refuses_a_change_of_columns_and_overwrite_applies_it(lake):
    """Keep one schema in append, and replace it when the dump is rebuilt in overwrite."""
    await lake.dump(batches=DAYS)

    with pytest.raises(SchemaMismatchError):
        await lake.dump(batches=DAYS, select=["id", "dia"])

    await lake.dump("overwrite", batches=DAYS, select=["id", "dia"])

    table = DeltaTable(lake.uri).to_pyarrow_table()
    assert table.column_names == ["id", "dia", "batch"]
    assert table.num_rows == len(ROWS)


async def test_overwrite_writes_every_window_again_and_sweeps_the_old_ones(lake):
    """Rewrite the same data in overwrite mode, and delete the windows outside the range."""
    await lake.dump(batches=DAYS)
    before = lake.writes()

    await lake.dump("overwrite", batches={**DAYS, "begin": "2024-03-02"})

    assert lake.writes() == before + 2 + NULL_WINDOW
    assert [row[0] for row in lake.rows()] == [3, 4]


async def test_overwrite_ignores_lookback_and_rebuilds_every_window(lake):
    """Rewrite all the windows in overwrite mode, even when lookback names only the last one."""
    await lake.dump(batches=DAYS)
    before = lake.writes()

    await lake.dump("overwrite", batches={**DAYS, "lookback": 1})

    assert lake.writes() == before + 3 + NULL_WINDOW


async def test_an_earlier_window_never_loaded_is_loaded_even_with_lookback(lake):
    """Let lookback limit the re-checks only, and not the first load of an older window."""
    await lake.dump(batches={**DAYS, "begin": "2024-03-02"})

    await lake.dump(batches={**DAYS, "lookback": 1})

    assert [row[0] for row in lake.rows()] == [1, 2, 3, 4]


async def test_a_new_filter_reaches_the_windows_that_lookback_does_not_recheck(lake):
    """Treat every window as not loaded after a new filter, so no old window keeps its rows."""
    await lake.dump(batches=DAYS)

    await lake.dump(batches={**DAYS, "lookback": 1}, filters=[{"column": "valor", "op": ">", "value": 15}])

    assert [row[0] for row in lake.rows()] == [2, 3, 4]


async def test_a_dump_that_writes_leaves_a_checkpoint_for_bigquery(lake):
    """Write a checkpoint after the windows, so a query does not replay a long log."""
    await lake.dump(batches=DAYS)

    log = Path(lake.uri, "_delta_log")
    assert any("checkpoint" in entry.name for entry in log.iterdir())


async def test_lookback_skips_the_windows_that_it_does_not_recheck(lake):
    """Do not read an old window again, but always load a window that is new."""
    await lake.dump(batches=DAYS)
    lake.put([{**ROWS[0], "valor": 11.0}, *ROWS[1:], {**ROWS[3], "id": 5, "dia": date(2024, 3, 4)}])
    wider = {**DAYS, "end": "2024-03-04", "lookback": 1}

    await lake.dump(batches=wider)

    by_id = {row[0]: row for row in lake.rows()}
    assert by_id[1][3] == 10.0
    assert 5 in by_id


async def test_without_batches_the_whole_source_is_one_window(lake):
    """Replace the whole table when the data changes, and skip it when it does not."""
    first = await lake.dump()
    assert await lake.dump() == first

    lake.put(ROWS[:2])
    await lake.dump()

    assert [row[0] for row in lake.rows()] == [1, 2]


@pytest.mark.parametrize(
    ("options", "message"),
    [
        pytest.param({"batches": {**DAYS, "column": "nada"}}, "must exist", id="a_batch_column_that_is_missing"),
        pytest.param(
            {"filters": [{"column": "nada", "op": "=", "value": 1}]},
            "Unknown columns",
            id="a_filter_on_a_missing_column",
        ),
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


def composed(value: TemplateValue) -> str:
    """Return a template value that is an SQL fragment, as text."""
    match value:
        case Composable():
            return value.as_string(None)
        case _:
            raise AssertionError(value)


@pytest.mark.parametrize(
    ("db_type", "read", "batched", "expected"),
    [
        pytest.param("postgres", {"table": "public.orders"}, True, 'FROM "src"."public"."orders"', id="a_table"),
        pytest.param("mysql", {"table": "app.orders"}, False, '"src"."app"."orders"', id="a_table_of_mysql"),
        pytest.param("postgres", {"query": "SELECT 1"}, False, "'SELECT 1'", id="a_native_query"),
    ],
)
def test_a_table_is_attached_and_a_query_is_a_literal(db_type, read, batched, expected):
    """Give an identifier for a table, and a literal for a native query."""
    values = read_values(db_type, read, batched)

    key = "table" if "table" in read else "query"
    assert expected.split("FROM ")[-1] == composed(values[key])


@pytest.mark.parametrize(
    ("db_type", "read", "batched", "message"),
    [
        pytest.param("postgres", {}, False, "exactly one", id="neither"),
        pytest.param("postgres", {"table": "a.b", "query": "SELECT 1"}, False, "exactly one", id="both"),
        pytest.param("postgres", {"query": "SELECT 1"}, True, "one window", id="windows_over_a_query"),
    ],
)
def test_a_read_that_does_not_fit_is_refused(db_type, read, batched, message):
    """Raise before any connection, so a typo never reaches the database."""
    with pytest.raises(ValueError, match=message):
        read_values(db_type, read, batched)


async def stored_rows(store, prefix: str) -> list[int]:
    """Return the ids of every Parquet object under a prefix."""
    ids: list[int] = []
    for key in await gcs.list_keys(store, prefix):
        data = bytes(await (await store.get_async(key)).bytes_async())
        ids += read_parquet(BytesIO(data))["id"].to_list()
    return sorted(ids)


@pytest.fixture
def batches_in_a_bucket(store, lake, monkeypatch):
    """Return a function that copies the lake in batches to an in-memory bucket."""
    monkeypatch.setattr(utils, "create_store", lambda *_: store)
    values = source_values("parquet", f"{lake.source}/*.parquet")

    async def run(dump_mode: DumpMode, batch_size: int) -> None:
        """Copy the four sample rows."""
        await dump_in_batches(FilesSource(), values, DESTINATION, dump_mode, batch_size)

    return run


async def test_a_source_is_copied_in_batches_of_the_given_size(store, batches_in_a_bucket):
    """Write one Parquet object for each batch of rows, and lose no row."""
    await batches_in_a_bucket("append", 3)

    keys = await gcs.list_keys(store, DESTINATION.prefix)
    assert len(keys) == 2
    assert await stored_rows(store, DESTINATION.prefix) == [1, 2, 3, 4]


async def test_append_keeps_the_old_objects_and_overwrite_deletes_them(store, batches_in_a_bucket):
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
        pytest.param({"batches": DAYS}, True, id="batches_over_a_source_read_whole"),
    ],
)
def test_a_source_read_whole_is_never_cut_into_windows(options, fails):
    """Refuse windows over a source that DuckDB cannot filter, so it is not read many times."""
    if fails:
        with pytest.raises(ValueError, match="one window"):
            without_batches(options, "A layer")
    else:
        assert without_batches(options, "A layer") == (options or {})


@pytest.mark.parametrize(
    ("dump_mode", "is_delta", "cleared"),
    [
        pytest.param("overwrite", False, True, id="overwrite_clears_the_files_of_an_older_layout"),
        pytest.param("overwrite", True, False, id="overwrite_never_deletes_a_delta_table"),
        pytest.param("append", False, False, id="append_never_deletes_anything"),
    ],
)
async def test_the_files_of_an_older_layout_go_before_an_overwrite_builds_a_delta_table(
    store, monkeypatch, dump_mode, is_delta, cleared
):
    """Delete the old Hive files only when an overwrite meets no Delta table."""
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
        order.append(f"dump:{args[2]}")

    monkeypatch.setattr(utils, "clear_legacy", fake_clear)
    monkeypatch.setattr(utils, "dump_to_delta", fake_dump)

    await load_to_delta(FilesSource(), {}, DESTINATION, "overwrite", None)

    assert order == ["clear:overwrite", "dump:gcs://b/staging/ds/tb"]


async def test_a_number_width_cuts_an_integer_column_into_runs_of_ids(lake):
    """Window by runs of ids, and keep every row."""
    commits = await lake.dump(batches={"column": "id", "width": 2, "begin": 1, "end": 4})

    assert commits == 2 + NULL_WINDOW
    assert [row[0] for row in lake.rows()] == [1, 2, 3, 4]


def naive(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> datetime:
    """Return a timestamp without a time zone, as a database column holds it."""
    return datetime(year, month, day, hour, minute, tzinfo=UTC).replace(tzinfo=None)


async def test_a_date_width_also_cuts_a_timestamp_column(lake):
    """Window by day over a timestamp, and keep the rows of a day whatever the time."""
    lake.put(
        [
            {"id": 1, "dia": naive(2024, 3, 1, 9), "nome": "a", "valor": 1.0},
            {"id": 2, "dia": naive(2024, 3, 1, 23, 59), "nome": "b", "valor": 2.0},
            {"id": 3, "dia": naive(2024, 3, 2, 0, 1), "nome": "c", "valor": 3.0},
        ]
    )

    commits = await lake.dump(batches=DAYS)

    assert commits == 3 + NULL_WINDOW
    assert [row[0] for row in lake.rows()] == [1, 2, 3]


@pytest.mark.parametrize(
    ("batches", "message"),
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
async def test_the_batch_column_must_have_the_type_that_the_windows_need(lake, batches, message):
    """Refuse a column of another type before any write, as a database would fail on the compare."""
    with pytest.raises(ValueError, match=message):
        await lake.dump(batches=batches)

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


@pytest.fixture(scope="module")
def delta_reader() -> DuckDBPyConnection:
    """Return a DuckDB connection with the delta extension, an independent reader of the log."""
    connection = duckdb_connect()
    try:
        connection.execute("INSTALL delta")
        connection.execute("LOAD delta")
    except Error:
        pytest.skip("The DuckDB delta extension is not available")
    return connection


async def after_first_load(lake: Lake) -> None:
    """Load the three days."""
    await lake.dump(batches=DAYS)


async def after_a_replaced_window(lake: Lake) -> None:
    """Change one row, so one day is replaced."""
    await lake.dump(batches=DAYS)
    lake.put([*ROWS[:2], {**ROWS[2], "valor": 99.0}, ROWS[3]])
    await lake.dump(batches=DAYS)


async def after_an_empty_window(lake: Lake) -> None:
    """Delete a whole day, so its rows are removed."""
    await lake.dump(batches=DAYS)
    lake.put(ROWS[:3])
    await lake.dump(batches=DAYS)


async def after_an_overwrite_with_other_columns(lake: Lake) -> None:
    """Rebuild with fewer columns, which replaces the schema."""
    await lake.dump(batches=DAYS)
    await lake.dump("overwrite", batches=DAYS, select=["id", "dia"])


@pytest.mark.extensions
@pytest.mark.parametrize(
    "scenario",
    [
        pytest.param(after_first_load, id="after_the_first_load"),
        pytest.param(after_a_replaced_window, id="after_a_replaced_window"),
        pytest.param(after_an_empty_window, id="after_an_empty_window"),
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
    frame = arrow_table(
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
    write_table(frame, tmp_path / "types.parquet")
    uri = str(tmp_path / "table")

    await dump_to_delta(FilesSource(), source_values("parquet", str(tmp_path / "types.parquet")), uri, "append", {})

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


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        pytest.param({"Authorization": "Bearer {TOKEN}"}, {"Authorization": "Bearer abc"}, id="a_name_in_the_text"),
        pytest.param({"k": "plain"}, {"k": "plain"}, id="no_name"),
        pytest.param(None, {}, id="no_values"),
    ],
)
def test_the_names_in_the_values_are_filled_from_the_secret(values, expected):
    """Put the secret in the value, and leave a plain value alone."""
    assert fill_secrets(values, {"TOKEN": "abc"}) == expected


def test_a_name_that_the_secret_lacks_is_a_key_error():
    """Raise, so a typo in a header fails before the request."""
    with pytest.raises(KeyError):
        fill_secrets({"k": "{OTHER}"}, {"TOKEN": "abc"})


async def test_json_lines_load_as_rows_and_nested_values_stay_nested(tmp_path):
    """Write the records, and read them back with DuckDB as typed columns."""
    folder = write_json_lines(
        [{"id": 1, "nome": "Ana", "tags": ["a", "b"]}, {"id": 2, "nome": "Bia", "tags": []}], tmp_path
    )
    uri = str(tmp_path / "table")

    await dump_to_delta(
        FilesSource(),
        source_values("json", f"{folder}/**/*.json"),
        uri,
        "append",
        {"capture": datetime(2024, 3, 10, 9, tzinfo=UTC)},
    )

    table = DeltaTable(uri).to_pyarrow_table().sort_by("id").to_pylist()
    assert [(row["id"], row["nome"], row["tags"]) for row in table] == [(1, "Ana", ["a", "b"]), (2, "Bia", [])]


async def test_a_query_is_read_into_one_frame(lake):
    """Return every row of the source in one Polars frame."""
    frame = await read_frame(FilesSource(), source_values("parquet", f"{lake.source}/*.parquet"))

    assert frame["id"].to_list() == [1, 2, 3, 4]


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
