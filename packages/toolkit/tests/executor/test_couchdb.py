from dataclasses import replace
from datetime import date
from json import loads

import pytest
from httpx2 import HTTPStatusError
from pyarrow import bool_ as arrow_bool
from pyarrow import json_ as arrow_json
from pyarrow import schema as arrow_schema
from pyarrow import string as arrow_string
from pyarrow.parquet import read_table
from pydantic import JsonValue

import toolkit.executor.sources.backends.couchdb as couchdb_module
from toolkit.chunks import NULL_CHUNK, Chunk, Rows
from toolkit.executor.engines.base import Engine
from toolkit.executor.engines.couchdb import CouchDBEngine
from toolkit.executor.engines.duckdb import DuckDBEngine
from toolkit.executor.requests import Table
from toolkit.executor.sources.backends.couchdb import CouchDBSource
from toolkit.executor.sources.models import CouchDBItem
from toolkit.executor.sources.protocols import DBConn
from toolkit.executor.sources.registry import get_source
from toolkit.executor.sources.shapes import Shape, convert_to_json_text
from toolkit.types import Chunks

USER = "user"
PASSWORD = "segredo"
DESIGN = "_design/cac500ad"


def change_row(item: CouchDBItem) -> dict[str, JsonValue]:
    """Return what a changes read gives for one item, to check each part of it."""
    return {
        "id": item.id,
        "seq": item.seq,
        "last_seq": item.last_seq,
        "deleted": item.deleted,
        "doc": convert_to_json_text(item.doc),
    }


CHANGES = Shape(
    arrow_schema(
        [
            ("id", arrow_string()),
            ("seq", arrow_string()),
            ("last_seq", arrow_string()),
            ("deleted", arrow_bool()),
            ("doc", arrow_json()),
        ]
    ),
    change_row,
)


async def test_a_database_reads_every_document_in_pages(couchdb, tmp_path, stage):
    """Read the whole database in pages of `page_size`, with no repeat and no gap."""
    store, conn = couchdb
    source = CouchDBSource(conn, batch_size=1000, secure=False)

    names, rows = await stage(source, tmp_path)

    assert (names, rows) == (["id", "doc"], 2500)
    assert (
        len([path for path, _, _ in store.requests if path.endswith("_all_docs")]) == 3
    )
    first = read_table(tmp_path / "out.parquet").slice(0, 1).to_pylist()[0]

    assert first["id"] == "00000000001"
    assert (
        first["doc"]
        == '{"_id":"00000000001","_rev":"1-00000000001","name":"NAME 00000000001"}'
    )


async def test_the_generic_shape_is_the_id_and_the_document(couchdb, tmp_path, stage):
    """Stage the two columns of the default shape."""
    _, conn = couchdb

    names, _ = await stage(CouchDBSource(conn, secure=False), tmp_path)

    assert names == ["id", "doc"]


async def test_a_source_with_another_shape_stages_its_columns(
    couchdb, tmp_path, shout_row, stage_rows
):
    """Take the columns and the rows of the shape that the source is given."""
    _, conn = couchdb
    shape = Shape(arrow_schema([("id", arrow_string())]), shout_row)
    source = CouchDBSource(conn, shape=shape, batch_size=2500, secure=False)

    await stage_rows(source, {"type": "couchdb"}, Rows(), tmp_path)
    table = read_table(tmp_path / "out.parquet")

    assert table["id"].to_pylist()[:2] == ['"00000000001"', '"00000000002"']


async def test_the_item_carries_the_update_sequence(couchdb, tmp_path, stage_rows):
    """Hand `last_seq` to the shape, which the generic one ignores."""
    store, conn = couchdb
    seen: list[str | None] = []

    def keep_row(item: CouchDBItem) -> dict[str, JsonValue]:
        """Record the update sequence of the item and return its id."""
        seen.append(item.last_seq)

        return {"id": item.id}

    shape = Shape(arrow_schema([("id", arrow_string())]), keep_row)
    source = CouchDBSource(conn, shape=shape, batch_size=2500, secure=False)

    await stage_rows(source, {"type": "couchdb"}, Rows(), tmp_path)

    assert set(seen) == {store.update_seq}


async def test_duckdb_reads_the_document_as_json(couchdb, tmp_path, stage):
    """Tag the document as JSON, so DuckDB reads the fields without a cast."""
    _, conn = couchdb

    await stage(CouchDBSource(conn, secure=False), tmp_path)
    path = tmp_path / "out.parquet"

    async with DuckDBEngine() as engine:
        [dataframe] = [
            batch
            async for batch in engine.query(
                f"SELECT typeof(doc) AS kind, doc->>'$.name' AS name FROM read_parquet('{path}') LIMIT 1"
            )
        ]

    assert dataframe.rows() == [("JSON", "NAME 00000000001")]


async def test_a_chunk_sends_an_exclusive_range_of_ids(couchdb, tmp_path, stage_rows):
    """Send the chunk as `startkey` and `endkey`, with the width of the ids."""
    store, conn = couchdb
    source = CouchDBSource(conn, secure=False)

    await stage_rows(
        source, {"type": "couchdb"}, Rows(chunk=Chunk("000000000002", 2, 4)), tmp_path
    )

    params = [seen for _, seen, _ in store.requests if "startkey" in seen][-1]

    assert params["startkey"] == '"00000000002"'
    assert params["endkey"] == '"00000000004"'
    assert params["inclusive_end"] == "false"
    assert read_table(tmp_path / "out.parquet").num_rows == 2


async def test_a_mango_selector_pages_by_bookmark(couchdb, tmp_path, stage):
    """Read the documents that a selector returns, page by page."""
    store, conn = couchdb
    source = CouchDBSource(conn, batch_size=1000, secure=False)

    names, rows = await stage(
        source, tmp_path, {"type": "couchdb", "query": '{"name": "NAME 00000000001"}'}
    )
    bodies = [body for path, _, body in store.requests if path.endswith("_find")]

    assert (names, rows) == (["id", "doc"], 2500)
    assert bodies[0] == {"selector": {"name": "NAME 00000000001"}, "limit": 1000}
    assert bodies[1]["bookmark"] == store.bookmark(1000)


async def test_a_selector_with_a_chunk_is_an_error(couchdb, tmp_path, stage_rows):
    """Raise for a chunk over a selector, which has no id to range over."""
    _, conn = couchdb
    source = CouchDBSource(conn, secure=False)

    with pytest.raises(ValueError, match="one chunk"):
        await stage_rows(
            source,
            {"type": "couchdb", "query": "{}"},
            Rows(chunk=Chunk("1", 1, 2)),
            tmp_path,
        )


ACTIVE = '{"situacao": "ativo"}'
ID_RANGE = {"_id": {"$gte": "00000000002", "$lt": "00000000004"}}


@pytest.mark.parametrize(
    ("chunk", "selector"),
    [
        pytest.param(None, {"$and": [{"situacao": "ativo"}]}, id="no_chunk"),
        pytest.param(
            Chunk("000000000002", 2, 4),
            {"$and": [{"situacao": "ativo"}, ID_RANGE]},
            id="a_chunk_is_a_range_of_ids",
        ),
        pytest.param(
            NULL_CHUNK,
            {
                "$and": [
                    {"situacao": "ativo"},
                    {"$or": [{"_id": {"$lt": "0"}}, {"_id": {"$gte": ":"}}]},
                ]
            },
            id="the_null_chunk_is_two_ranges",
        ),
    ],
)
async def test_filters_read_by_find_with_the_chunk_as_a_range_of_ids(
    couchdb, tmp_path, chunk, selector, stage_rows
):
    """Join the filters and the ids of the chunk in one selector for `_find`."""
    store, conn = couchdb
    source = CouchDBSource(conn, batch_size=3000, secure=False)
    rows = Rows(filters=(ACTIVE,), column="id" if chunk else None, chunk=chunk)

    await stage_rows(source, {"type": "couchdb"}, rows, tmp_path)
    [body] = [body for path, _, body in store.requests if path.endswith("_find")]

    assert body["selector"] == selector


async def test_two_filters_are_two_conditions_of_one_selector(
    couchdb, tmp_path, stage_rows
):
    """Join every filter with `$and`, in the order that they come."""
    store, conn = couchdb
    source = CouchDBSource(conn, batch_size=3000, secure=False)
    filters = (ACTIVE, '{"idade": {"$gte": 18}}')

    await stage_rows(source, {"type": "couchdb"}, Rows(filters=filters), tmp_path)
    [body] = [body for path, _, body in store.requests if path.endswith("_find")]

    assert body["selector"] == {
        "$and": [{"situacao": "ativo"}, {"idade": {"$gte": 18}}]
    }


async def test_a_filtered_read_skips_the_design_documents(
    couchdb, tmp_path, stage_rows
):
    """Keep a design document out of the rows, as an `_all_docs` read does."""
    store, conn = couchdb
    store.docs[DESIGN] = {"_id": DESIGN, "_rev": "1-a"}
    source = CouchDBSource(conn, batch_size=3000, secure=False)

    await stage_rows(source, {"type": "couchdb"}, Rows(filters=(ACTIVE,)), tmp_path)
    ids = read_table(tmp_path / "out.parquet")["id"].to_pylist()

    assert DESIGN not in ids
    assert len(ids) == 2500


@pytest.mark.parametrize(
    ("values", "filters", "column", "message"),
    [
        pytest.param(
            {"query": "{}"}, (ACTIVE,), None, "drop the query", id="filters_and_query"
        ),
        pytest.param(
            {"since": "0"}, (ACTIVE,), None, "drop the query", id="filters_and_since"
        ),
        pytest.param(
            {}, ("not json",), None, "not a JSON selector", id="a_filter_that_is_text"
        ),
        pytest.param(
            {}, ("[1]",), None, "not a JSON selector", id="a_filter_that_is_a_list"
        ),
        pytest.param(
            {}, (), "name", "set chunks.column to 'id'", id="a_chunk_over_a_field"
        ),
    ],
)
async def test_a_read_that_does_not_fit_the_source_is_refused(
    couchdb, tmp_path, values, filters, column, message, stage_rows
):
    """Raise for filters that are no selectors, and for a chunk over a field."""
    _, conn = couchdb
    source = CouchDBSource(conn, secure=False)
    rows = Rows(filters=filters, column=column)

    with pytest.raises(ValueError, match=message):
        await stage_rows(source, {"type": "couchdb", **values}, rows, tmp_path)


@pytest.mark.parametrize(
    ("column", "width", "message"),
    [
        pytest.param("id", 100, None, id="the_id_is_the_column_of_the_chunks"),
        pytest.param("name", 100, "cuts the id", id="any_other_column_is_refused"),
        pytest.param("id", "month", "number of ids", id="a_date_width_is_refused"),
    ],
)
def test_the_couchdb_engine_checks_that_the_chunks_cut_the_id(column, width, message):
    """Accept the id, and refuse another column or a date width before any read."""
    engine = CouchDBEngine(CouchDBSource(DBConn("h", 1, "d", USER, PASSWORD)))
    chunks = Chunks(column=column, width=width, begin=0)
    described = {"id": "VARCHAR", "name": "VARCHAR"}

    if message is None:
        engine.check_chunks(chunks, described)
    else:
        with pytest.raises(ValueError, match=message):
            engine.check_chunks(chunks, described)


async def test_a_selector_that_is_not_json_is_an_error(couchdb, tmp_path, stage_rows):
    """Raise for a query that is not a Mango selector, and name the error."""
    _, conn = couchdb
    source = CouchDBSource(conn, secure=False)

    with pytest.raises(ValueError, match="not a JSON selector"):
        await stage_rows(source, {"type": "couchdb", "query": "name"}, Rows(), tmp_path)


async def test_a_wrong_login_raises_the_http_error(couchdb, tmp_path, stage):
    """Raise the error of the 401 that the proxy answers, and read no page."""
    store, conn = couchdb
    wrong = DBConn(conn.hostname, conn.port, conn.database, USER, "errada")

    with pytest.raises(HTTPStatusError, match="401"):
        await stage(CouchDBSource(wrong, secure=False), tmp_path)

    assert [path for path, _, _ in store.requests] == ["/people/"]


async def test_a_wrong_path_fails_when_the_engine_opens(
    proxied_couchdb, tmp_path, stage
):
    """Ask for the database on open, so a refused path reads no document."""
    store, conn = proxied_couchdb

    with pytest.raises(HTTPStatusError, match="404"):
        await stage(CouchDBSource(conn, secure=False), tmp_path)

    assert [path for path, _, _ in store.requests] == ["/people/"]


async def test_the_login_never_reaches_the_url():
    """Send the login in the header only, and use https for any port but 5984."""
    secure = DBConn("10.2.231.241", 6984, "people", USER, PASSWORD)
    plain = DBConn("10.2.231.241", 5984, "people", USER, PASSWORD)
    source = CouchDBSource(secure)

    assert source.base_url == "https://10.2.231.241:6984"
    assert PASSWORD not in source.base_url
    assert CouchDBSource(plain).base_url == "http://10.2.231.241:5984"


async def test_the_tls_setting_reaches_the_client(couchdb, monkeypatch):
    """Pass the TLS setting and the login to the client."""
    _, conn = couchdb
    seen: dict[str, object] = {}

    class FakeClient:
        def __init__(self, **kwargs: object) -> None:
            seen.update(kwargs)

    monkeypatch.setattr(couchdb_module, "AsyncClient", FakeClient)
    CouchDBSource(conn, verify_tls=False, secure=False).build_client()

    assert seen["verify"] is False
    assert seen["auth"] == (USER, PASSWORD)


def test_a_source_that_needs_a_connection_names_it():
    """Raise for a source with no connection, and name the missing `conn`."""
    with pytest.raises(ValueError, match="conn"):
        get_source("couchdb")()


async def test_regression_a_design_document_is_not_a_row(couchdb, tmp_path, stage):
    """Regression: the design docs sort after the ids, so a read took them as rows."""
    store, conn = couchdb
    store.docs[DESIGN] = {"_id": DESIGN, "_rev": "1-design"}

    _, rows = await stage(CouchDBSource(conn, batch_size=2500, secure=False), tmp_path)

    assert rows == 2500
    assert DESIGN not in read_table(tmp_path / "out.parquet")["id"].to_pylist()


async def test_regression_a_page_does_not_repeat_the_key_of_the_page_before(
    small_couchdb, tmp_path, stage
):
    """Regression: `startkey` includes its key, so the last row of a page came again."""
    store, conn = small_couchdb
    source = CouchDBSource(conn, batch_size=3, secure=False)

    _, rows = await stage(source, tmp_path)
    ids = read_table(tmp_path / "out.parquet")["id"].to_pylist()

    assert (rows, len(set(ids)), ids) == (7, 7, sorted(store.docs))
    assert [seen.get("skip") for _, seen, _ in store.requests if "skip" in seen] == [
        "1",
        "1",
    ]


async def test_regression_the_last_numeric_id_is_read_below_the_colon(
    couchdb, tmp_path, stage_rows
):
    """Regression: a design document was the last key, so a chunk could not be cut."""
    store, conn = couchdb
    store.docs[DESIGN] = {"_id": DESIGN, "_rev": "1-design"}
    store.docs["CPF-20261020011517"] = {"_id": "CPF-20261020011517", "_rev": "1-a"}
    source = CouchDBSource(conn, secure=False)

    await stage_rows(
        source,
        {"type": "couchdb"},
        Rows(chunk=Chunk("1", 0, 10_000_000_000)),
        tmp_path,
    )
    ids = read_table(tmp_path / "out.parquet")["id"].to_pylist()

    assert (len(ids), ids[0], ids[-1]) == (2500, "00000000001", "00000002500")


async def test_regression_ids_that_are_not_one_width_are_an_error(
    couchdb, tmp_path, stage_rows
):
    """Regression: mixed id widths made a chunk read the wrong range."""
    store, conn = couchdb
    store.docs = {
        "00000000": {"_id": "00000000", "_rev": "1-a"},
        "99017782000139": {"_id": "99017782000139", "_rev": "1-b"},
    }
    source = CouchDBSource(conn, secure=False)

    with pytest.raises(ValueError, match="not zero-padded numbers"):
        await stage_rows(
            source,
            {"type": "couchdb"},
            Rows(chunk=Chunk("1", 0, 100_000_000)),
            tmp_path,
        )


async def test_an_empty_database_keeps_the_columns(empty_couchdb, tmp_path, stage):
    """Write a file with the columns and no row, so that DuckDB still knows them."""
    _, conn = empty_couchdb

    names, rows = await stage(CouchDBSource(conn, secure=False), tmp_path)

    assert (names, rows) == (["id", "doc"], 0)


async def test_regression_a_database_name_with_a_slash_is_quoted_in_the_path(
    couchdb, tmp_path, stage
):
    """Regression: the slash of a database name split the path of the read."""
    store, conn = couchdb
    archive = DBConn(conn.hostname, conn.port, "people/archive", USER, PASSWORD)

    await stage(CouchDBSource(archive, batch_size=2500, secure=False), tmp_path)

    assert next(
        path for path, _, _ in store.requests if path.endswith("_all_docs")
    ) == ("/people%2Farchive/_all_docs")


@pytest.mark.parametrize(
    ("prefix", "expected"),
    [
        pytest.param("", "https://db.example:6984", id="no_prefix"),
        pytest.param(
            "couchdb", "https://db.example:6984/couchdb", id="without_slashes"
        ),
        pytest.param(
            "/couchdb/", "https://db.example:6984/couchdb", id="with_both_slashes"
        ),
        pytest.param("/a/b", "https://db.example:6984/a/b", id="two_levels"),
    ],
)
def test_the_path_prefix_joins_the_url_without_doubled_slashes(prefix, expected):
    """Join the path prefix to the server with one slash."""
    conn = DBConn("db.example", 6984, "people", USER, PASSWORD)

    assert CouchDBSource(conn, path_prefix=prefix).base_url == expected


async def test_a_path_prefix_reaches_every_endpoint(proxied_couchdb, tmp_path, stage):
    """Send the three kinds of read under the prefix that the proxy publishes."""
    store, conn = proxied_couchdb
    source = CouchDBSource(conn, path_prefix="/couchdb", secure=False)

    await stage(source, tmp_path)
    await stage(source, tmp_path, {"type": "couchdb", "query": "{}"})
    await stage(source, tmp_path, {"type": "couchdb", "since": "0"})

    assert {path for path, _, _ in store.requests} == {
        "/couchdb/people/",
        "/couchdb/people/_all_docs",
        "/couchdb/people/_find",
        "/couchdb/people/_changes",
    }

    with pytest.raises(HTTPStatusError, match="404"):
        await stage(CouchDBSource(conn, secure=False), tmp_path)


def test_the_registry_path_takes_its_options_from_the_settings(monkeypatch):
    """Build the options of a source from the settings, so no code has to pass them."""
    monkeypatch.setenv("COUCHDB_PAGE_SIZE", "5")
    monkeypatch.setenv("COUCHDB_VERIFY_TLS", "false")
    monkeypatch.setenv("COUCHDB_SECURE", "false")
    monkeypatch.setenv("COUCHDB_PATH_PREFIX", "/couchdb")
    conn = DBConn("db.example", 6984, "people", USER, PASSWORD)

    source = CouchDBSource(conn)

    assert (
        source.batch_size,
        source.verify_tls,
        source.secure,
        source.path_prefix,
    ) == (
        5,
        False,
        False,
        "/couchdb",
    )
    assert source.base_url == "http://db.example:6984/couchdb"


async def test_a_changes_read_stages_every_change_in_pages(
    small_couchdb, tmp_path, stage
):
    """Read the changes from the start, page by page, with the sequence of each one."""
    store, conn = small_couchdb
    source = CouchDBSource(conn, shape=CHANGES, batch_size=3, secure=False)

    await stage(source, tmp_path, {"type": "couchdb", "since": "0"})
    rows = read_table(tmp_path / "out.parquet").to_pylist()

    assert [row["id"] for row in rows] == sorted(store.docs)
    assert [row["seq"] for row in rows] == [
        f"{number}-g1AAAA" for number in range(1, 8)
    ]
    assert [row["last_seq"] for row in rows] == ["3-g1AAAA"] * 3 + ["6-g1AAAA"] * 3 + [
        "7-g1AAAA"
    ]
    assert {row["deleted"] for row in rows} == {False}
    assert [
        seen["since"] for path, seen, _ in store.requests if path.endswith("_changes")
    ] == [
        "0",
        "3-g1AAAA",
        "6-g1AAAA",
    ]


async def test_a_changes_read_after_a_sequence_gives_only_what_changed(
    small_couchdb, tmp_path, stage
):
    """Give the updated document and the tombstone of the deleted one only."""
    store, conn = small_couchdb
    store.touch("00000000002")
    store.remove("00000000003")
    source = CouchDBSource(conn, shape=CHANGES, secure=False)

    await stage(source, tmp_path, {"type": "couchdb", "since": "7-g1AAAA"})
    rows = read_table(tmp_path / "out.parquet").to_pylist()

    assert [(row["id"], row["seq"], row["deleted"]) for row in rows] == [
        ("00000000002", "8-g1AAAA", False),
        ("00000000003", "9-g1AAAA", True),
    ]
    assert loads(rows[0]["doc"])["name"] == "RENAMED"
    assert loads(rows[1]["doc"]) == {
        "_id": "00000000003",
        "_rev": "2-00000000003",
        "_deleted": True,
    }


async def test_a_changes_read_with_nothing_new_keeps_the_columns(
    small_couchdb, tmp_path, stage
):
    """Write a file with the columns and no row after the last sequence."""
    _, conn = small_couchdb

    names, rows = await stage(
        CouchDBSource(conn, shape=CHANGES, secure=False),
        tmp_path,
        {"type": "couchdb", "since": "7-g1AAAA"},
    )

    assert (names, rows) == (CHANGES.columns.names, 0)


async def test_a_changes_read_skips_the_design_documents(
    small_couchdb, tmp_path, stage
):
    """Leave the design documents out of the changes, as out of a database read."""
    store, conn = small_couchdb
    store.docs[DESIGN] = {"_id": DESIGN, "_rev": "1-design"}
    store.counter += 1
    store.sequence[DESIGN] = store.counter

    _, rows = await stage(
        CouchDBSource(conn, secure=False), tmp_path, {"type": "couchdb", "since": "0"}
    )

    assert rows == 7


@pytest.mark.parametrize(
    ("values", "chunk", "message"),
    [
        pytest.param(
            {"since": "0"}, Chunk("1", 1, 2), "one chunk", id="since_and_chunk"
        ),
    ],
)
async def test_a_changes_read_refuses_what_it_cannot_combine(
    couchdb, tmp_path, values, chunk, message, stage_rows
):
    """Raise for a changes read that also asks for a chunk."""
    _, conn = couchdb
    source = CouchDBSource(conn, secure=False)

    with pytest.raises(ValueError, match=message):
        await stage_rows(
            source, {"type": "couchdb", **values}, Rows(chunk=chunk), tmp_path
        )


async def test_regression_the_last_chunk_reaches_the_top_of_the_ids(
    couchdb, tmp_path, stage_rows
):
    """Regression: a chunk that ended at 10^width had an end key before its start."""
    store, conn = couchdb
    store.docs["50000000000"] = {"_id": "50000000000", "_rev": "1-a"}
    store.docs["99999999999"] = {"_id": "99999999999", "_rev": "1-b"}
    source = CouchDBSource(conn, batch_size=3000, secure=False)

    await stage_rows(
        source, {"type": "couchdb"}, Rows(chunk=Chunk("x", 0, 10**11)), tmp_path
    )
    ids = read_table(tmp_path / "out.parquet")["id"].to_pylist()

    assert (len(ids), ids[-2:]) == (2502, ["50000000000", "99999999999"])


async def test_the_chunks_and_the_null_chunk_read_every_document_once(
    couchdb, tmp_path, stage_rows
):
    """Read the numeric ranges and the null chunk, and get each document one time."""
    store, conn = couchdb
    extra = [
        "50000000000",
        "99999999999",
        "CPF-20261020011517",
        "SUCESSAO-1",
        "-legacy",
    ]
    store.docs.update({key: {"_id": key, "_rev": "1-a"} for key in extra})
    store.docs[DESIGN] = {"_id": DESIGN, "_rev": "1-design"}
    source = CouchDBSource(conn, batch_size=3000, secure=False)
    read: list[str] = []

    for number, chunk in enumerate(
        [Chunk("a", 0, 5 * 10**10), Chunk("b", 5 * 10**10, 10**11), NULL_CHUNK]
    ):
        folder = tmp_path / str(number)
        folder.mkdir()
        await stage_rows(source, {"type": "couchdb"}, Rows(chunk=chunk), folder)
        ids = read_table(folder / "out.parquet")["id"].to_pylist()
        read += [str(key) for key in ids]

    assert sorted(read) == sorted(key for key in store.docs if key != DESIGN)


async def test_a_key_width_cuts_ids_of_mixed_length_by_their_first_digits(
    couchdb, tmp_path, stage_rows
):
    """Cut ids of 8 and 14 digits by their first three digits, with no width probe."""
    store, conn = couchdb
    keys = [
        "00000000",
        "00000000123456",
        "12345678",
        "12345678901234",
        "55555555",
        "99017782000139",
    ]
    store.docs = {key: {"_id": key, "_rev": "1-a"} for key in keys}
    source = CouchDBSource(conn, key_width=3, batch_size=3000, secure=False)
    halves: list[list[str]] = []

    for number, chunk in enumerate([Chunk("a", 0, 500), Chunk("b", 500, 1000)]):
        folder = tmp_path / str(number)
        folder.mkdir()
        await stage_rows(source, {"type": "couchdb"}, Rows(chunk=chunk), folder)
        ids = read_table(folder / "out.parquet")["id"].to_pylist()
        halves.append([str(key) for key in ids])

    assert halves == [keys[:4], keys[4:]]
    assert "1" not in [seen.get("limit") for _, seen, _ in store.requests]


async def test_a_chunk_of_dates_is_an_error(couchdb, tmp_path, stage_rows):
    """Raise for a chunk of dates, because a CouchDB read cuts ids."""
    _, conn = couchdb
    chunk = Chunk("2024-01-01", date(2024, 1, 1), date(2024, 1, 2))

    with pytest.raises(ValueError, match="needs numbers"):
        await stage_rows(
            CouchDBSource(conn, secure=False),
            {"type": "couchdb"},
            Rows(chunk=chunk),
            tmp_path,
        )


async def test_the_table_value_names_the_database(couchdb, tmp_path, stage):
    """Read the database that `table` names, or the one of the connection."""
    store, conn = couchdb
    source = CouchDBSource(conn, batch_size=3000, secure=False)

    await stage(source, tmp_path, {"type": "couchdb", "table": "archive"})
    await stage(source, tmp_path)

    assert [path for path, _, _ in store.requests] == [
        "/people/",
        "/archive/_all_docs",
        "/people/",
        "/people/_all_docs",
    ]


def test_the_options_of_a_context_choose_the_shape_and_the_key_width(shapes):
    """Take the shape from its `module:name` path, and the key width as it comes."""
    conn = DBConn("db.example", 6984, "people", USER, PASSWORD)
    source = get_source("couchdb")(
        conn=conn, shape="shapes_of_the_test:SHOUT", key_width=3
    )

    assert isinstance(source, CouchDBSource)
    assert (source.shape, source.key_width) == (shapes, 3)


def test_the_batch_size_of_the_options_is_the_page_size():
    """Take the documents of a page from the options, and keep the setting without."""
    conn = DBConn("db.example", 6984, "people", USER, PASSWORD)

    sized = CouchDBSource(conn, batch_size=5000)
    plain = CouchDBSource(conn)

    assert (sized.batch_size, plain.batch_size) == (5000, 10_000)


def test_a_source_without_options_is_the_default_source():
    """Keep the default shape and detect the width of the ids."""
    source = CouchDBSource(DBConn("db.example", 6984, "people", USER, PASSWORD))

    assert (source.shape.columns.names, source.key_width) == (["id", "doc"], None)


@pytest.mark.parametrize(
    ("options", "message"),
    [
        pytest.param({"shape": "no_colon"}, "module:name", id="path_without_a_colon"),
        pytest.param(
            {"shape": "missing_module_of_the_test:X"},
            "cannot be imported",
            id="module_that_does_not_exist",
        ),
        pytest.param(
            {"shape": "shapes_of_the_test:MISSING"},
            "is not a Shape",
            id="name_that_does_not_exist",
        ),
        pytest.param(
            {"shape": "shapes_of_the_test:NOT_A_SHAPE"},
            "is not a Shape",
            id="name_of_another_type",
        ),
        pytest.param(
            {"key_width": "three"}, "key_width", id="key_width_that_is_not_a_number"
        ),
        pytest.param({"width": 3}, "width", id="option_that_does_not_exist"),
        pytest.param({"batch_size": 0}, "batch_size", id="batch_size_of_zero"),
    ],
)
def test_options_that_do_not_fit_are_an_error(shapes, options, message):
    """Raise for a shape path that leads to no shape, and for an unknown option."""
    conn = DBConn("db.example", 6984, "people", USER, PASSWORD)

    with pytest.raises(ValueError, match=message):
        get_source("couchdb")(conn=conn, **options)


async def test_the_id_width_is_read_once_per_engine_and_not_kept_by_the_source(
    couchdb, tmp_path
):
    """Probe the ids once for the chunks of an engine, and again for a new one."""
    store, conn = couchdb
    source = CouchDBSource(conn, secure=False)
    chunks = (Chunk("000000000001", 1, 2), Chunk("000000000002", 2, 3))
    origin = Table(conn.database)

    def probes() -> int:
        """Count the requests that read one key."""
        return sum(1 for _, seen, _ in store.requests if seen.get("limit") == "1")

    reading = replace(source, origin=origin)

    async with Engine.from_source(reading) as engine:
        for chunk in chunks:
            rows = Rows(column="id", chunk=chunk)
            await engine.materialize(tmp_path / "out.parquet", rows)

    assert probes() == 2

    async with Engine.from_source(reading) as engine:
        rows = Rows(column="id", chunk=chunks[0])
        await engine.materialize(tmp_path / "out.parquet", rows)

    assert probes() == 4
    assert source == CouchDBSource(conn, secure=False)
