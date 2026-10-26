from collections.abc import AsyncGenerator, Callable
from datetime import date
from json import dumps
from pathlib import Path

import pytest
from duckdb import IOException
from httpx2 import HTTPError
from oracledb import DatabaseError
from polars import DataFrame
from pymongo.errors import PyMongoError
from pymongoarrow.api import Schema

from toolkit.chunks import NULL_CHUNK, Chunk, ChunkPlan, Probe, Rows, hash_summary
from toolkit.executor.engines.couchdb import CouchDBEngine
from toolkit.executor.engines.duckdb import DuckDBEngine
from toolkit.executor.engines.mongodb import MongoDBEngine
from toolkit.executor.engines.oracle import OracleEngine
from toolkit.executor.executor import Executor
from toolkit.executor.probe import (
    build_native_chunk,
    build_range_pipeline,
    quote_native,
)
from toolkit.executor.render import render_template
from toolkit.executor.requests import Aggregate, Query, Table
from toolkit.executor.sources.backends.couchdb import CouchDBSource
from toolkit.executor.sources.backends.files import FilesSource
from toolkit.executor.sources.backends.mongodb import MongoDBSource
from toolkit.executor.sources.backends.mssql import MSSQLSource
from toolkit.executor.sources.backends.mysql import MySQLSource
from toolkit.executor.sources.backends.oracle import OracleSource
from toolkit.executor.sources.backends.postgres import PostgresSource
from toolkit.executor.sources.protocols import DBConn
from toolkit.executor.types import Origin

PROBE = Probe("test-1", "updated_at")
KEYS = ["w0", "w1", "w2"]
CHUNKS = [Chunk(key, 0, 1) for key in KEYS]


def build_plan(known: dict[str, int | None]) -> Callable[[Chunk], ChunkPlan]:
    """Return a `plan` that gives the ledger number of each chunk."""

    def plan(chunk: Chunk) -> ChunkPlan:
        return ChunkPlan(Rows(column=chunk.key), [], known.get(chunk.key))

    return plan


async def run(
    engine, known: dict[str, int | None], *, probe: Probe | None = PROBE
) -> tuple[list[str], list[tuple[str, int | None]]]:
    """Run the three chunks, and return the skipped keys and what `handle` got."""
    handled: list[tuple[str, int | None]] = []

    async def handle(chunk: Chunk, folder: Path, token: int | None) -> None:
        handled.append((chunk.key, token))

    skipped = await Executor(engine).run_chunks(
        CHUNKS, build_plan(known), handle, probe=probe
    )

    return sorted(chunk.key for chunk in skipped), sorted(
        handled, key=lambda item: item[0]
    )


@pytest.mark.parametrize(
    ("probes", "known", "skipped", "handled"),
    [
        pytest.param(
            {"w0": 5, "w1": 6, "w2": 7},
            {"w0": 5, "w1": 6, "w2": 7},
            ["w0", "w1", "w2"],
            [],
            id="a_token_that_is_the_known_one_skips_the_chunk",
        ),
        pytest.param(
            {"w0": 5, "w1": 6, "w2": 7},
            {"w0": 5, "w1": 1, "w2": None},
            ["w0"],
            [("w1", 6), ("w2", 7)],
            id="a_changed_or_unknown_chunk_downloads_and_keeps_its_token",
        ),
        pytest.param(
            {"w0": None, "w1": 6, "w2": 7},
            {"w0": 5, "w1": 6, "w2": 7},
            ["w1", "w2"],
            [("w0", None)],
            id="a_probe_that_cannot_tell_downloads_with_no_token",
        ),
        pytest.param(
            None,
            {"w0": 5, "w1": 6, "w2": 7},
            [],
            [("w0", None), ("w1", None), ("w2", None)],
            id="an_engine_without_a_probe_always_downloads",
        ),
    ],
)
async def test_the_probe_decides_which_chunks_are_downloaded(
    fake_chunk_engine, probes, known, skipped, handled
) -> None:
    """Skip what the probe says is unchanged, and give the token to the others."""
    engine = fake_chunk_engine(probes=probes)

    assert await run(engine, known) == (skipped, handled)
    assert sorted(engine.activity.materialized) == sorted(key for key, _ in handled)


async def test_a_skipped_chunk_is_never_downloaded(fake_chunk_engine) -> None:
    """Leave the source alone for a chunk that the probe skips."""
    engine = fake_chunk_engine(probes={"w0": 5, "w1": 6, "w2": 7})

    await run(engine, {"w0": 5, "w1": 99, "w2": 7})

    assert engine.activity.materialized == ["w1"]


async def test_no_probe_is_asked_when_the_dump_turns_it_off(fake_chunk_engine) -> None:
    """Download everything and ask the engine nothing, as an overwrite does."""
    engine = fake_chunk_engine(probes={"w0": 5, "w1": 6, "w2": 7})

    result = await run(engine, {"w0": 5, "w1": 6, "w2": 7}, probe=None)

    assert result == ([], [("w0", None), ("w1", None), ("w2", None)])
    assert engine.activity.probed == []


async def test_each_worker_keeps_the_token_of_its_own_chunk(fake_chunk_engine) -> None:
    """Give every chunk its own token, also when three workers probe together."""
    probes = {"w0": 10, "w1": 11, "w2": 12}
    engine = fake_chunk_engine(workers=3, probes=probes)

    _, handled = await run(engine, {})

    assert handled == [("w0", 10), ("w1", 11), ("w2", 12)]


@pytest.mark.parametrize(
    ("source", "kind", "needs_column"),
    [
        pytest.param(
            CouchDBSource, "couchdb-rev-1", False, id="couchdb_uses_its_revision"
        ),
        pytest.param(OracleSource, "oracle-rowscn-1", False, id="oracle_uses_its_scn"),
        pytest.param(
            PostgresSource, "pg-range-1", True, id="postgres_needs_a_range_column"
        ),
        pytest.param(
            MySQLSource, "mysql-range-1", True, id="mysql_needs_a_range_column"
        ),
        pytest.param(
            MSSQLSource, "mssql-range-1", True, id="mssql_needs_a_range_column"
        ),
        pytest.param(
            MongoDBSource, "mongodb-range-1", True, id="mongodb_needs_a_range_column"
        ),
        pytest.param(FilesSource, "", False, id="files_have_no_probe"),
    ],
)
def test_each_source_declares_the_kind_of_its_probe_and_whether_it_needs_a_column(
    source, kind, needs_column
) -> None:
    """Name the token of every source, and say which ones need a column."""
    assert source.probe_kind == kind
    assert source.range_column_needed is needs_column


NUMBERS = Chunk("000000000100", 100, 200)
DAYS = Chunk("2024-01-01", date(2024, 1, 1), date(2024, 1, 2))


@pytest.mark.parametrize(
    ("dialect", "name", "expected"),
    [
        pytest.param("postgres", "id", '"id"', id="postgres_double_quotes"),
        pytest.param("postgres", 'a"b', '"a""b"', id="postgres_doubles_a_quote"),
        pytest.param("mysql", "id", "`id`", id="mysql_backticks"),
        pytest.param("mysql", "a`b", "`a``b`", id="mysql_doubles_a_backtick"),
        pytest.param("mssql", "id", "[id]", id="mssql_brackets"),
        pytest.param("mssql", "a]b", "[a]]b]", id="mssql_doubles_a_bracket"),
        pytest.param("mssql", "a b", "[a b]", id="a_space_is_kept"),
    ],
)
def test_a_name_is_quoted_in_the_dialect(dialect, name, expected) -> None:
    """Quote with the characters of the dialect, and double the closing one."""
    assert quote_native(dialect, name) == expected


@pytest.mark.parametrize(
    ("dialect", "rows", "expected"),
    [
        pytest.param(
            "postgres",
            Rows(column="id", chunk=NUMBERS),
            '"id" >= 100 AND "id" < 200',
            id="a_number_range_is_exact",
        ),
        pytest.param(
            "mysql",
            Rows(column="d", chunk=DAYS),
            "`d` >= '2023-12-31' AND `d` < '2024-01-03'",
            id="a_date_range_grows_one_day_on_each_side",
        ),
        pytest.param(
            "mssql",
            Rows(column="d", chunk=DAYS),
            "[d] >= '20231231' AND [d] < '20240103'",
            id="mssql_dates_have_no_dashes",
        ),
        pytest.param(
            "postgres",
            Rows(column="id", chunk=NULL_CHUNK),
            '"id" IS NULL',
            id="the_null_chunk_is_the_null_rows",
        ),
        pytest.param("postgres", Rows(), "", id="no_chunk_is_the_whole_table"),
        pytest.param(
            "postgres",
            Rows(column="id"),
            "",
            id="a_column_with_no_chunk_is_the_whole_table",
        ),
        pytest.param(
            "postgres",
            Rows(filters=["a > 1"], column="id", chunk=NUMBERS),
            None,
            id="filters_are_duckdb_sql_so_there_is_no_native_chunk",
        ),
    ],
)
def test_the_chunk_becomes_a_native_predicate(dialect, rows, expected) -> None:
    """Write the chunk in the dialect, wider for dates, and none for filters."""
    assert build_native_chunk(dialect, rows) == expected


DUCKDB_CONN = DBConn("host", 5432, "db", "user", "password")
DUCKDB_CHUNK = Rows(column="id", chunk=Chunk("a", 0, 100))
DUCKDB_TOKEN = hash_summary(10, "2024-03-02", "")
DUCKDB_ANSWER = {"n": [10], "last": ["2024-03-02"]}


def build_duckdb_engine(
    monkeypatch, source, answer=None, *, fails=False
) -> tuple[DuckDBEngine, list[str]]:
    """Return an engine over a source, whose queries are recorded and answered."""
    engine = DuckDBEngine(source)
    sent: list[str] = []

    async def query(sql: str, **_: object) -> AsyncGenerator[DataFrame]:
        sent.append(sql)

        if fails:
            raise IOException("the query timed out")

        yield DataFrame(DUCKDB_ANSWER if answer is None else answer)

    monkeypatch.setattr(engine, "query", query)

    return engine, sent


def postgres(origin: Origin | None = None) -> PostgresSource:
    """Return a Postgres source over an origin, the table `public.items` by default."""
    return PostgresSource(conn=DUCKDB_CONN, origin=origin or Table("public.items"))


@pytest.mark.parametrize(
    ("source_class", "function", "table"),
    [
        pytest.param(
            PostgresSource, "postgres_query", '"public"."items"', id="postgres"
        ),
        pytest.param(MySQLSource, "mysql_query", "`public`.`items`", id="mysql"),
        pytest.param(MSSQLSource, "mssql_scan", "[public].[items]", id="mssql"),
    ],
)
async def test_the_probe_runs_on_the_server_through_the_function_of_the_extension(
    monkeypatch, source_class, function, table
) -> None:
    """Send the native query to the pass-through function of the dialect."""
    engine, sent = build_duckdb_engine(
        monkeypatch, source_class(conn=DUCKDB_CONN, origin=Table("public.items"))
    )

    await engine.probe(DUCKDB_CHUNK, None, PROBE)

    assert len(sent) == 1
    assert function in sent[0]
    assert table.replace("'", "''") in sent[0]
    assert 'MAX("updated_at")' in sent[0] or "updated_at" in sent[0]


@pytest.mark.parametrize(
    ("dialect", "count"),
    [
        pytest.param("postgres", "COUNT(*) AS n", id="postgres_counts_with_count"),
        pytest.param("mysql", "COUNT(*) AS n", id="mysql_counts_with_count"),
        pytest.param("mssql", "COUNT_BIG(*) AS n", id="mssql_counts_with_count_big"),
    ],
)
def test_each_dialect_ranges_with_the_count_and_the_highest_value(
    dialect, count
) -> None:
    """Render a native query that reads no row: the count and `MAX` of the column."""
    names = {"postgres": '"{}"', "mysql": "`{}`", "mssql": "[{}]"}[dialect]
    values = {
        "type": dialect,
        "table": names.format("public") + "." + names.format("items"),
        "column": names.format("updated_at"),
        "predicate": f"{names.format('id')} >= 0",
    }

    sql = render_template("duckdb/probe_range", values)

    assert count in sql
    assert f"MAX({names.format('updated_at')}) AS last" in sql
    assert f"WHERE {names.format('id')} >= 0" in sql


@pytest.mark.parametrize(
    ("known", "answer", "download"),
    [
        pytest.param(
            DUCKDB_TOKEN,
            None,
            False,
            id="the_same_count_and_highest_value_skips_the_chunk",
        ),
        pytest.param(
            DUCKDB_TOKEN,
            {"n": [11], "last": ["2024-03-02"]},
            True,
            id="a_new_row_changes_the_count",
        ),
        pytest.param(
            DUCKDB_TOKEN,
            {"n": [10], "last": ["2024-03-03"]},
            True,
            id="an_update_moves_the_highest_value",
        ),
        pytest.param(None, None, True, id="no_known_token_downloads"),
    ],
)
async def test_the_probe_compares_the_count_and_the_highest_value_with_the_ledger(
    monkeypatch, known, answer, download
) -> None:
    """Download unless the count and the highest value are the known ones."""
    engine, _ = build_duckdb_engine(monkeypatch, postgres(), answer)

    assert await engine.probe(DUCKDB_CHUNK, known, PROBE) is download
    assert engine.token == (
        hash_summary(int(answer["n"][0]), answer["last"][0], "")
        if answer
        else DUCKDB_TOKEN
    )


@pytest.mark.parametrize(
    ("source", "rows", "probe"),
    [
        pytest.param(
            FilesSource(origin=Table("x")),
            DUCKDB_CHUNK,
            PROBE,
            id="a_source_with_no_probe",
        ),
        pytest.param(
            postgres(Query("select 1", "postgres")),
            DUCKDB_CHUNK,
            PROBE,
            id="a_native_query_is_not_a_table",
        ),
        pytest.param(
            postgres(),
            Rows(filters=["a > 1"], column="id", chunk=Chunk("a", 0, 100)),
            PROBE,
            id="filters_are_duckdb_sql",
        ),
        pytest.param(postgres(), DUCKDB_CHUNK, Probe("pg-range-1"), id="no_column"),
    ],
)
async def test_what_a_probe_cannot_cover_is_downloaded_with_no_query(
    monkeypatch, source, rows, probe
) -> None:
    """Download without a token, and send nothing to the server."""
    engine, sent = build_duckdb_engine(monkeypatch, source)

    assert await engine.probe(rows, DUCKDB_TOKEN, probe) is True
    assert engine.token is None
    assert sent == []


async def test_a_failed_probe_downloads_the_chunk_and_never_raises(monkeypatch) -> None:
    """Turn a DuckDB error, such as a timeout, into a download with no token."""
    engine, sent = build_duckdb_engine(monkeypatch, postgres(), fails=True)

    assert await engine.probe(DUCKDB_CHUNK, DUCKDB_TOKEN, PROBE) is True
    assert await engine.probe(DUCKDB_CHUNK, DUCKDB_TOKEN, PROBE) is True
    assert engine.token is None
    assert len(sent) == 2


COUCHDB_WHOLE = Rows(column="id", chunk=Chunk("a", 0, 100))


def build_couchdb_source(conn, **options) -> CouchDBSource:
    """Return a source over the fake database, in the database `people`."""
    return CouchDBSource(conn, secure=False, origin=Table("people"), **options)


async def take_couchdb_token(conn, rows=COUCHDB_WHOLE, **options) -> int | None:
    """Probe a chunk with no known token, and return the token."""
    async with CouchDBEngine(build_couchdb_source(conn, **options)) as engine:
        await engine.probe(rows, None, PROBE)

        return engine.token


async def test_the_probe_skips_a_chunk_that_did_not_change(small_couchdb) -> None:
    """Give the same token twice, and skip the chunk that the ledger holds."""
    _, conn = small_couchdb
    token = await take_couchdb_token(conn)

    async with CouchDBEngine(build_couchdb_source(conn)) as engine:
        assert await engine.probe(COUCHDB_WHOLE, token, PROBE) is False
        assert engine.token == token
        assert await engine.probe(COUCHDB_WHOLE, None, PROBE) is True


@pytest.mark.parametrize(
    "change",
    [
        pytest.param(lambda store: store.touch("00000000003"), id="a_new_revision"),
        pytest.param(
            lambda store: store.remove("00000000003"), id="a_deleted_document"
        ),
        pytest.param(
            lambda store: store.docs.update(
                {"00000000050": {"_id": "00000000050", "_rev": "1-a"}}
            ),
            id="a_new_document",
        ),
    ],
)
async def test_a_changed_document_changes_the_token_and_downloads_the_chunk(
    small_couchdb, change
) -> None:
    """Download after a write, a delete or an insert in the chunk."""
    store, conn = small_couchdb
    before = await take_couchdb_token(conn)

    change(store)

    async with CouchDBEngine(build_couchdb_source(conn)) as engine:
        assert await engine.probe(COUCHDB_WHOLE, before, PROBE) is True
        assert engine.token is not None
        assert engine.token != before


async def test_a_change_outside_the_chunk_leaves_its_token_alone(small_couchdb) -> None:
    """Look only at the documents between the keys of the chunk."""
    store, conn = small_couchdb
    store.docs["00000000500"] = {"_id": "00000000500", "_rev": "1-a"}
    before = await take_couchdb_token(conn)

    store.touch("00000000500")

    assert await take_couchdb_token(conn) == before


async def test_a_design_document_is_never_part_of_the_token(small_couchdb) -> None:
    """Leave out the design documents, as a read does."""
    store, conn = small_couchdb
    before = await take_couchdb_token(conn, Rows())

    store.docs["_design/x"] = {"_id": "_design/x", "_rev": "1-a"}

    assert await take_couchdb_token(conn, Rows()) == before


async def test_the_token_does_not_depend_on_the_size_of_a_page(small_couchdb) -> None:
    """Give the same token for a page of 2 documents and for a page of 1000."""
    _, conn = small_couchdb

    assert await take_couchdb_token(conn, batch_size=2) == await take_couchdb_token(
        conn, batch_size=1000
    )


async def test_the_null_chunk_joins_its_two_ranges(small_couchdb) -> None:
    """Read the keys below `0` and from `:`, and move when one of them changes."""
    store, conn = small_couchdb
    rows = Rows(column="id", chunk=NULL_CHUNK)
    empty = await take_couchdb_token(conn, rows)
    store.docs["abc"] = {"_id": "abc", "_rev": "1-a"}

    with_letters = await take_couchdb_token(conn, rows)

    assert empty is not None
    assert with_letters != empty


@pytest.mark.parametrize(
    ("rows", "origin"),
    [
        pytest.param(
            Rows(filters=['{"a": 1}'], column="id", chunk=Chunk("a", 0, 100)),
            Table("people"),
            id="filters",
        ),
        pytest.param(
            Rows(column="other", chunk=Chunk("a", 0, 100)),
            Table("people"),
            id="a_column_other_than_the_id",
        ),
        pytest.param(
            COUCHDB_WHOLE, Query("{}", table="people"), id="a_selector_origin"
        ),
    ],
)
async def test_what_a_probe_cannot_cover_is_downloaded_with_no_request(
    small_couchdb, rows, origin
) -> None:
    """Download without a token, and ask the server for no listing."""
    store, conn = small_couchdb
    source = CouchDBSource(conn, secure=False, origin=origin)

    async with CouchDBEngine(source) as engine:
        before = len(store.requests)

        assert await engine.probe(rows, 1, PROBE) is True
        assert engine.token is None
        assert len(store.requests) == before


async def test_a_server_error_downloads_the_chunk_and_never_raises(
    small_couchdb, monkeypatch
) -> None:
    """Turn an HTTP error into a download with no token."""
    _, conn = small_couchdb

    async def fail(*arguments: object) -> tuple[int, str]:
        raise HTTPError("the server went away")

    async with CouchDBEngine(build_couchdb_source(conn)) as engine:
        monkeypatch.setattr(engine.client, "hash_revisions", fail)

        assert await engine.probe(COUCHDB_WHOLE, 1, PROBE) is True
        assert engine.token is None


ORACLE_CONN = DBConn("host", 1521, "SVC", "user", "password")
ORACLE_TOKEN = hash_summary(10, "999", "")
ORACLE_RANGE = Rows(column="ID", chunk=Chunk("a", 100, 200))


def build_oracle_engine(
    monkeypatch,
    origin: Origin | None = None,
    *,
    scn: int | None = 123,
    fails: bool = False,
) -> tuple[OracleEngine, list[tuple[str, list[object]]]]:
    """Return an engine over a table, whose queries are recorded and answered."""
    engine = OracleEngine(
        OracleSource(conn=ORACLE_CONN, origin=origin or Table("APP.ORDERS"))
    )
    engine.scn = scn
    sent: list[tuple[str, list[object]]] = []

    async def query(sql: str, *, params=None, **_: object) -> AsyncGenerator[DataFrame]:
        sent.append((sql, list(params or [])))

        if fails:
            raise DatabaseError(
                "ORA-08181: specified number is not a valid system change number"
            )

        yield DataFrame({"n": ["10"], "total": ["999"]})

    monkeypatch.setattr(engine, "query", query)

    return engine, sent


@pytest.mark.parametrize(
    ("scn", "rows", "as_of", "params"),
    [
        pytest.param(
            123,
            ORACLE_RANGE,
            True,
            [123, 100, 200],
            id="read_as_of_the_scn_of_the_dump",
        ),
        pytest.param(
            None, ORACLE_RANGE, False, [100, 200], id="no_scn_reads_the_current_table"
        ),
        pytest.param(123, Rows(), True, [123], id="the_whole_table"),
        pytest.param(
            123,
            Rows(column="ID", chunk=NULL_CHUNK),
            True,
            [123],
            id="the_null_chunk_has_no_bind",
        ),
        pytest.param(
            123,
            Rows(column="DIA", chunk=Chunk("d", date(2024, 3, 1), date(2024, 3, 2))),
            True,
            [123, date(2024, 3, 1), date(2024, 3, 2)],
            id="a_date_chunk_binds_dates",
        ),
    ],
)
async def test_the_probe_reads_the_table_at_the_point_of_the_dump(
    monkeypatch, scn, rows, as_of, params
) -> None:
    """Count the chunk and take its highest SCN, with the binds of the extract."""
    engine, sent = build_oracle_engine(monkeypatch, scn=scn)

    await engine.probe(rows, None, PROBE)

    sql, bound = sent[0]
    assert ("AS OF SCN :scn" in sql) is as_of
    assert bound == params
    assert 'FROM "APP"."ORDERS"' in sql
    assert "COUNT(*)" in sql
    assert "MAX(ORA_ROWSCN)" in sql


async def test_the_filters_are_the_oracle_conditions_of_the_extract(
    monkeypatch,
) -> None:
    """Put the filters in the probe, because they are Oracle SQL already."""
    engine, sent = build_oracle_engine(monkeypatch)

    await engine.probe(
        Rows(filters=["STATUS <> 'X'"], column="ID", chunk=Chunk("a", 100, 200)),
        None,
        PROBE,
    )

    sql, _ = sent[0]
    assert "(STATUS <> 'X') AND" in sql
    assert '"ID" >= :p0' in sql


@pytest.mark.parametrize(
    ("known", "download"),
    [
        pytest.param(ORACLE_TOKEN, False, id="the_known_token_skips_the_chunk"),
        pytest.param(ORACLE_TOKEN + 1, True, id="another_token_downloads"),
        pytest.param(None, True, id="no_known_token_downloads_and_keeps_the_token"),
    ],
)
async def test_oracle_the_probe_compares_its_token_with_the_one_of_the_ledger(
    monkeypatch, known, download
) -> None:
    """Download unless the token of the server is the one that the ledger holds."""
    engine, _ = build_oracle_engine(monkeypatch)

    assert await engine.probe(ORACLE_RANGE, known, PROBE) is download
    assert engine.token == ORACLE_TOKEN


async def test_a_query_origin_is_downloaded_with_no_query(monkeypatch) -> None:
    """Skip the probe for a query, because ORA_ROWSCN does not cross it."""
    engine, sent = build_oracle_engine(monkeypatch, Query("SELECT * FROM T"))

    assert await engine.probe(ORACLE_RANGE, ORACLE_TOKEN, PROBE) is True
    assert engine.token is None
    assert sent == []


async def test_oracle_a_failed_probe_downloads_the_chunk_and_never_raises(
    monkeypatch,
) -> None:
    """Turn a database error into a download with no token."""
    engine, sent = build_oracle_engine(monkeypatch, fails=True)

    assert await engine.probe(ORACLE_RANGE, ORACLE_TOKEN, PROBE) is True
    assert await engine.probe(ORACLE_RANGE, ORACLE_TOKEN, PROBE) is True
    assert engine.token is None
    assert len(sent) == 2


async def test_a_filter_that_is_not_one_condition_downloads_the_chunk(
    monkeypatch,
) -> None:
    """Leave the error of a bad filter to the extract, and download."""
    engine, sent = build_oracle_engine(monkeypatch)

    assert (
        await engine.probe(
            Rows(filters=["1 = 1; DROP TABLE T"], column="ID", chunk=Chunk("a", 0, 1)),
            ORACLE_TOKEN,
            PROBE,
        )
        is True
    )
    assert engine.token is None
    assert sent == []


MONGODB_CONN = DBConn("host", 27017, "lab", "user", "password")
MONGODB_SHAPE = Schema({"_id": int, "name": str, "tags": [str]})
MONGODB_TOKEN = hash_summary(10, "2024-03-02", "")
MONGODB_CHUNK = Rows(column="id", chunk=Chunk("a", 0, 100))
MONGODB_ANSWER: list[dict[str, object]] = [{"_id": None, "n": 10, "last": "2024-03-02"}]


def build_mongodb_engine(
    monkeypatch, origin=None, answer=None, *, fails=False
) -> tuple[MongoDBEngine, list[tuple[str, list[dict[str, object]]]]]:
    """Return an engine whose probe aggregation is recorded and answered."""
    source = MongoDBSource(
        conn=MONGODB_CONN, shape=MONGODB_SHAPE, origin=origin or Table("people")
    )
    engine = MongoDBEngine(source)
    sent: list[tuple[str, list[dict[str, object]]]] = []

    async def run_probe(collection: str, pipeline) -> list[dict[str, object]]:
        sent.append((collection, list(pipeline)))

        if fails:
            raise PyMongoError("the server went away")

        return MONGODB_ANSWER if answer is None else answer

    monkeypatch.setattr(engine, "run_probe", run_probe)

    return engine, sent


def test_the_range_pipeline_counts_and_takes_the_highest_value_of_the_field() -> None:
    """Group the chunk into its count and the `$max` of one field."""
    assert build_range_pipeline("updatedAt", {"a": 1}) == [
        {"$match": {"a": 1}},
        {
            "$group": {
                "_id": None,
                "n": {"$sum": 1},
                "last": {"$max": "$updatedAt"},
            }
        },
    ]


def test_an_empty_query_has_no_match_stage() -> None:
    """Leave the `$match` out when there is nothing to match."""
    assert len(build_range_pipeline("updatedAt", {})) == 1


async def test_the_probe_aggregates_the_chunk_on_the_server(monkeypatch) -> None:
    """Send the chunk, the filters and the `$max` of the column to the collection."""
    engine, sent = build_mongodb_engine(monkeypatch)

    await engine.probe(
        Rows(filters=['{"ativo": true}'], column="id", chunk=Chunk("a", 0, 100)),
        None,
        PROBE,
    )

    collection, pipeline = sent[0]
    assert collection == "people"
    assert pipeline[0]["$match"] == {
        "$and": [{"ativo": True}, {"id": {"$gte": 0, "$lt": 100}}],
    }
    assert dumps(pipeline[1]).count('"$max": "$updated_at"') == 1


@pytest.mark.parametrize(
    ("known", "download"),
    [
        pytest.param(MONGODB_TOKEN, False, id="the_known_token_skips_the_chunk"),
        pytest.param(MONGODB_TOKEN + 1, True, id="another_token_downloads"),
        pytest.param(None, True, id="no_known_token_downloads_and_keeps_the_token"),
    ],
)
async def test_mongodb_the_probe_compares_its_token_with_the_one_of_the_ledger(
    monkeypatch, known, download
) -> None:
    """Download unless the count and the highest value are the known ones."""
    engine, _ = build_mongodb_engine(monkeypatch)

    assert await engine.probe(MONGODB_CHUNK, known, PROBE) is download
    assert engine.token == MONGODB_TOKEN


async def test_a_nested_schema_is_probed_because_no_text_of_the_fields_is_read(
    monkeypatch,
) -> None:
    """Probe a collection whose schema has a list: the probe reads one column only."""
    engine, sent = build_mongodb_engine(monkeypatch)

    assert await engine.probe(MONGODB_CHUNK, MONGODB_TOKEN, PROBE) is False
    assert len(sent) == 1


async def test_a_chunk_with_no_document_has_the_token_of_zero_rows(monkeypatch) -> None:
    """Take a token from an empty answer, so an emptied chunk is noticed."""
    engine, _ = build_mongodb_engine(monkeypatch, answer=[])

    assert await engine.probe(MONGODB_CHUNK, MONGODB_TOKEN, PROBE) is True
    assert engine.token == hash_summary(0, "None", "")


async def test_the_null_chunk_matches_the_documents_with_no_value(monkeypatch) -> None:
    """Probe the null chunk as the read does."""
    engine, sent = build_mongodb_engine(monkeypatch)

    await engine.probe(Rows(column="id", chunk=NULL_CHUNK), None, PROBE)

    assert sent[0][1][0]["$match"] == {"$and": [{"id": None}]}


@pytest.mark.parametrize(
    ("origin", "probe"),
    [
        pytest.param(Aggregate("[]", "people"), PROBE, id="an_aggregation"),
        pytest.param(Table("people"), Probe("mongodb-range-1"), id="no_column"),
    ],
)
async def test_what_a_probe_cannot_cover_is_downloaded_with_no_aggregation(
    monkeypatch, origin, probe
) -> None:
    """Download without a token, and send nothing to the server."""
    engine, sent = build_mongodb_engine(monkeypatch, origin)

    assert await engine.probe(MONGODB_CHUNK, MONGODB_TOKEN, probe) is True
    assert engine.token is None
    assert sent == []


async def test_a_query_origin_is_probed_in_its_collection(monkeypatch) -> None:
    """Join the selector of the query with the chunk."""
    engine, sent = build_mongodb_engine(
        monkeypatch, Query('{"ativo": true}', table="people")
    )

    await engine.probe(MONGODB_CHUNK, None, PROBE)

    assert sent[0][1][0]["$match"] == {
        "$and": [{"ativo": True}, {"id": {"$gte": 0, "$lt": 100}}]
    }


async def test_mongodb_a_server_error_downloads_the_chunk_and_never_raises(
    monkeypatch,
) -> None:
    """Turn a PyMongo error into a download with no token."""
    engine, sent = build_mongodb_engine(monkeypatch, fails=True)

    assert await engine.probe(MONGODB_CHUNK, MONGODB_TOKEN, PROBE) is True
    assert await engine.probe(MONGODB_CHUNK, MONGODB_TOKEN, PROBE) is True
    assert engine.token is None
    assert len(sent) == 2


async def test_a_filter_that_is_not_json_downloads_the_chunk(monkeypatch) -> None:
    """Leave the error of a bad filter to the extract, and download."""
    engine, sent = build_mongodb_engine(monkeypatch)

    assert (
        await engine.probe(
            Rows(filters=["not json"], column="id", chunk=Chunk("a", 0, 1)),
            MONGODB_TOKEN,
            PROBE,
        )
        is True
    )
    assert engine.token is None
    assert sent == []
