from datetime import date
from json import loads
from typing import cast

import pytest
from httpx2 import HTTPStatusError
from pyarrow import bool_ as arrow_bool
from pyarrow import json_ as arrow_json
from pyarrow import schema as arrow_schema
from pyarrow import string as arrow_string
from pyarrow.parquet import read_table
from pydantic import JsonValue

import toolkit.executor.sources.backends.couchdb as couchdb_module
from toolkit.batches import NULL_WINDOW, Rows, Window
from toolkit.executor.engines.duckdb import DuckDBEngine
from toolkit.executor.engines.protocol import Engine
from toolkit.executor.sources.backends.couchdb import (
    CouchDBSource,
    Shape,
    json_text,
)
from toolkit.executor.sources.models import CouchDBItem
from toolkit.executor.sources.protocols import DBConn, SourceContext
from toolkit.settings import settings

USER = "user"
PASSWORD = "segredo"
DESIGN = "_design/cac500ad"
CLIENT = cast("Engine", cast("object", None))
"""The source stages a file and never calls the DuckDB client."""


def shout_row(item: CouchDBItem) -> dict[str, JsonValue]:
    """Return the id of one item in capitals, for a source with another shape."""
    return {"id": json_text(item.id.upper())}


def change_row(item: CouchDBItem) -> dict[str, JsonValue]:
    """Return what a changes read gives for one item, to check each part of it."""
    return {
        "id": item.id,
        "seq": item.seq,
        "last_seq": item.last_seq,
        "deleted": item.deleted,
        "doc": json_text(item.doc),
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


async def stage(source: CouchDBSource, folder, values=None) -> tuple[list[str], int]:
    """Stage a read and return the columns and the count of rows."""
    await source.stage(CLIENT, values or {"type": source.name}, Rows(), folder)
    table = read_table(folder / "_stage" / f"{source.name}.parquet")

    return table.schema.names, table.num_rows


async def test_a_database_reads_every_document_in_pages(couchdb, tmp_path):
    """Read the whole database in pages of `page_size`, with no repeat and no gap."""
    store, conn = couchdb
    source = CouchDBSource(conn, page_size=1000, secure=False)

    names, rows = await stage(source, tmp_path)

    assert (names, rows) == (["id", "doc"], 2500)
    assert (
        len([path for path, _, _ in store.requests if path.endswith("_all_docs")]) == 3
    )
    first = (
        read_table(tmp_path / "_stage" / "couchdb.parquet").slice(0, 1).to_pylist()[0]
    )

    assert first["id"] == "00000000001"
    assert (
        first["doc"]
        == '{"_id":"00000000001","_rev":"1-00000000001","name":"NAME 00000000001"}'
    )


async def test_the_generic_shape_is_the_id_and_the_document(couchdb, tmp_path):
    """Stage the two columns of the default shape."""
    _, conn = couchdb

    names, _ = await stage(CouchDBSource(conn, secure=False), tmp_path)

    assert names == ["id", "doc"]


async def test_a_source_with_another_shape_stages_its_columns(couchdb, tmp_path):
    """Take the columns and the rows of the shape that the source is given."""
    _, conn = couchdb
    shape = Shape(arrow_schema([("id", arrow_string())]), shout_row)
    source = CouchDBSource(conn, shape=shape, page_size=2500, secure=False)

    await source.stage(CLIENT, {"type": "couchdb"}, Rows(), tmp_path)
    table = read_table(tmp_path / "_stage" / "couchdb.parquet")

    assert table["id"].to_pylist()[:2] == ['"00000000001"', '"00000000002"']


async def test_the_item_carries_the_update_sequence(couchdb, tmp_path):
    """Hand `last_seq` to the shape, which the generic one ignores."""
    store, conn = couchdb
    seen: list[str | None] = []

    def keep_row(item: CouchDBItem) -> dict[str, JsonValue]:
        """Record the update sequence of the item and return its id."""
        seen.append(item.last_seq)

        return {"id": item.id}

    shape = Shape(arrow_schema([("id", arrow_string())]), keep_row)
    source = CouchDBSource(conn, shape=shape, page_size=2500, secure=False)

    await source.stage(CLIENT, {"type": "couchdb"}, Rows(), tmp_path)

    assert set(seen) == {store.update_seq}


async def test_duckdb_reads_the_document_as_json(couchdb, tmp_path):
    """Tag the document as JSON, so DuckDB reads the fields without a cast."""
    _, conn = couchdb

    await stage(CouchDBSource(conn, secure=False), tmp_path)
    path = tmp_path / "_stage" / "couchdb.parquet"

    async with DuckDBEngine.connect() as engine:
        [frame] = [
            batch
            async for batch in engine.query(
                f"SELECT typeof(doc) AS kind, doc->>'$.name' AS name FROM read_parquet('{path}') LIMIT 1"
            )
        ]

    assert frame.rows() == [("JSON", "NAME 00000000001")]


async def test_a_window_sends_an_exclusive_range_of_ids(couchdb, tmp_path):
    """Send the window as `startkey` and `endkey`, with the width of the ids."""
    store, conn = couchdb
    source = CouchDBSource(conn, secure=False)

    await source.stage(
        CLIENT, {"type": "couchdb"}, Rows(window=Window("000000000002", 2, 4)), tmp_path
    )

    params = [seen for _, seen, _ in store.requests if "startkey" in seen][-1]

    assert params["startkey"] == '"00000000002"'
    assert params["endkey"] == '"00000000004"'
    assert params["inclusive_end"] == "false"
    assert read_table(tmp_path / "_stage" / "couchdb.parquet").num_rows == 2


async def test_a_mango_selector_pages_by_bookmark(couchdb, tmp_path):
    """Read the documents that a selector returns, page by page."""
    store, conn = couchdb
    source = CouchDBSource(conn, page_size=1000, secure=False)

    names, rows = await stage(
        source, tmp_path, {"type": "couchdb", "query": '{"name": "NAME 00000000001"}'}
    )
    bodies = [body for path, _, body in store.requests if path.endswith("_find")]

    assert (names, rows) == (["id", "doc"], 2500)
    assert bodies[0] == {"selector": {"name": "NAME 00000000001"}, "limit": 1000}
    assert bodies[1]["bookmark"] == store.bookmark(1000)


async def test_a_selector_with_a_window_is_an_error(couchdb, tmp_path):
    """Raise for a window over a selector, which has no id to range over."""
    _, conn = couchdb
    source = CouchDBSource(conn, secure=False)

    with pytest.raises(ValueError, match="one window"):
        await source.stage(
            CLIENT,
            {"type": "couchdb", "query": "{}"},
            Rows(window=Window("1", 1, 2)),
            tmp_path,
        )


async def test_a_filter_is_an_error(couchdb, tmp_path):
    """Raise for filters, which belong in a Mango selector."""
    _, conn = couchdb
    source = CouchDBSource(conn, secure=False)
    rows = Rows(filters=({"column": "name", "op": "=", "value": "1"},))

    with pytest.raises(ValueError, match="no filters"):
        await source.stage(CLIENT, {"type": "couchdb"}, rows, tmp_path)


async def test_a_selector_that_is_not_json_is_an_error(couchdb, tmp_path):
    """Raise for a query that is not a Mango selector, and name the error."""
    _, conn = couchdb
    source = CouchDBSource(conn, secure=False)

    with pytest.raises(ValueError, match="not a JSON selector"):
        await source.stage(
            CLIENT, {"type": "couchdb", "query": "name"}, Rows(), tmp_path
        )


async def test_a_wrong_login_raises_the_http_error(couchdb, tmp_path):
    """Raise the error of the 401 that the proxy answers, and read no page."""
    _, conn = couchdb
    wrong = DBConn(conn.hostname, conn.port, conn.database, USER, "errada")

    with pytest.raises(HTTPStatusError, match="401"):
        await stage(CouchDBSource(wrong, secure=False), tmp_path)


async def test_the_login_never_reaches_the_url():
    """Send the login in the header only, and use https for any port but 5984."""
    secure = DBConn("10.2.231.241", 6984, "people", USER, PASSWORD)
    plain = DBConn("10.2.231.241", 5984, "people", USER, PASSWORD)
    source = CouchDBSource.from_context(SourceContext(secure))

    assert source.base_url == "https://10.2.231.241:6984"
    assert PASSWORD not in source.base_url
    assert (
        CouchDBSource.from_context(SourceContext(plain)).base_url
        == "http://10.2.231.241:5984"
    )


async def test_the_tls_setting_reaches_the_client(couchdb, monkeypatch):
    """Pass the TLS setting and the login to the client."""
    _, conn = couchdb
    seen: dict[str, object] = {}

    class FakeClient:
        def __init__(self, **kwargs: object) -> None:
            seen.update(kwargs)

    monkeypatch.setattr(couchdb_module, "AsyncClient", FakeClient)
    CouchDBSource(conn, verify_tls=False, secure=False).client()

    assert seen["verify"] is False
    assert seen["auth"] == (USER, PASSWORD)


async def test_a_source_that_needs_a_connection_names_itself():
    """Raise for a context with no connection, and register the source."""
    from toolkit.executor.sources.registry import load_sources

    assert "couchdb" in load_sources()

    with pytest.raises(ValueError, match="Source couchdb needs connection data"):
        CouchDBSource.from_context(SourceContext(None))


async def test_regression_a_design_document_is_not_a_row(couchdb, tmp_path):
    """Regression: the design docs sort after the ids, so a read took them as rows."""
    store, conn = couchdb
    store.docs[DESIGN] = {"_id": DESIGN, "_rev": "1-design"}

    _, rows = await stage(CouchDBSource(conn, page_size=2500, secure=False), tmp_path)

    assert rows == 2500
    assert (
        DESIGN
        not in read_table(tmp_path / "_stage" / "couchdb.parquet")["id"].to_pylist()
    )


async def test_regression_a_page_does_not_repeat_the_key_of_the_page_before(
    small_couchdb, tmp_path
):
    """Regression: `startkey` includes its key, so the last row of a page came again."""
    store, conn = small_couchdb
    source = CouchDBSource(conn, page_size=3, secure=False)

    _, rows = await stage(source, tmp_path)
    ids = read_table(tmp_path / "_stage" / "couchdb.parquet")["id"].to_pylist()

    assert (rows, len(set(ids)), ids) == (7, 7, sorted(store.docs))
    assert [seen.get("skip") for _, seen, _ in store.requests if "skip" in seen] == [
        "1",
        "1",
    ]


async def test_regression_the_last_numeric_id_is_read_below_the_colon(
    couchdb, tmp_path
):
    """Regression: a design document was the last key, so a window could not be cut."""
    store, conn = couchdb
    store.docs[DESIGN] = {"_id": DESIGN, "_rev": "1-design"}
    store.docs["CPF-20261020011517"] = {"_id": "CPF-20261020011517", "_rev": "1-a"}
    source = CouchDBSource(conn, secure=False)

    await source.stage(
        CLIENT,
        {"type": "couchdb"},
        Rows(window=Window("1", 0, 10_000_000_000)),
        tmp_path,
    )
    ids = read_table(tmp_path / "_stage" / "couchdb.parquet")["id"].to_pylist()

    assert (len(ids), ids[0], ids[-1]) == (2500, "00000000001", "00000002500")


async def test_regression_ids_that_are_not_one_width_are_an_error(couchdb, tmp_path):
    """Regression: mixed id widths made a window read the wrong range."""
    store, conn = couchdb
    store.docs = {
        "00000000": {"_id": "00000000", "_rev": "1-a"},
        "99017782000139": {"_id": "99017782000139", "_rev": "1-b"},
    }
    source = CouchDBSource(conn, secure=False)

    with pytest.raises(ValueError, match="not zero-padded numbers"):
        await source.stage(
            CLIENT,
            {"type": "couchdb"},
            Rows(window=Window("1", 0, 100_000_000)),
            tmp_path,
        )


async def test_an_empty_database_keeps_the_columns(empty_couchdb, tmp_path):
    """Write a file with the columns and no row, so that DuckDB still knows them."""
    _, conn = empty_couchdb

    names, rows = await stage(CouchDBSource(conn, secure=False), tmp_path)

    assert (names, rows) == (["id", "doc"], 0)


async def test_regression_a_database_name_with_a_slash_is_quoted_in_the_path(
    couchdb, tmp_path
):
    """Regression: the slash of a database name split the path of the read."""
    store, conn = couchdb
    archive = DBConn(conn.hostname, conn.port, "people/archive", USER, PASSWORD)

    await stage(CouchDBSource(archive, page_size=2500, secure=False), tmp_path)

    assert store.requests[0][0] == "/people%2Farchive/_all_docs"


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


async def test_a_path_prefix_reaches_every_endpoint(proxied_couchdb, tmp_path):
    """Send the three kinds of read under the prefix that the proxy publishes."""
    store, conn = proxied_couchdb
    source = CouchDBSource(conn, path_prefix="/couchdb", secure=False)

    await stage(source, tmp_path)
    await stage(source, tmp_path, {"type": "couchdb", "query": "{}"})
    await stage(source, tmp_path, {"type": "couchdb", "since": "0"})

    assert {path for path, _, _ in store.requests} == {
        "/couchdb/people/_all_docs",
        "/couchdb/people/_find",
        "/couchdb/people/_changes",
    }

    with pytest.raises(HTTPStatusError, match="404"):
        await stage(CouchDBSource(conn, secure=False), tmp_path)


def test_the_registry_path_takes_its_options_from_the_settings(monkeypatch):
    """Build the options of a source from the settings, so no code has to pass them."""
    monkeypatch.setattr(settings, "COUCHDB_PAGE_SIZE", 5)
    monkeypatch.setattr(settings, "COUCHDB_VERIFY_TLS", False)
    monkeypatch.setattr(settings, "COUCHDB_SECURE", False)
    monkeypatch.setattr(settings, "COUCHDB_PATH_PREFIX", "/couchdb")
    conn = DBConn("db.example", 6984, "people", USER, PASSWORD)

    source = CouchDBSource.from_context(SourceContext(conn))

    assert (source.page_size, source.verify_tls, source.secure, source.path_prefix) == (
        5,
        False,
        False,
        "/couchdb",
    )
    assert source.base_url == "http://db.example:6984/couchdb"


async def test_a_changes_read_stages_every_change_in_pages(small_couchdb, tmp_path):
    """Read the changes from the start, page by page, with the sequence of each one."""
    store, conn = small_couchdb
    source = CouchDBSource(conn, shape=CHANGES, page_size=3, secure=False)

    await stage(source, tmp_path, {"type": "couchdb", "since": "0"})
    rows = read_table(tmp_path / "_stage" / "couchdb.parquet").to_pylist()

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
    small_couchdb, tmp_path
):
    """Give the updated document and the tombstone of the deleted one only."""
    store, conn = small_couchdb
    store.touch("00000000002")
    store.remove("00000000003")
    source = CouchDBSource(conn, shape=CHANGES, secure=False)

    await stage(source, tmp_path, {"type": "couchdb", "since": "7-g1AAAA"})
    rows = read_table(tmp_path / "_stage" / "couchdb.parquet").to_pylist()

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
    small_couchdb, tmp_path
):
    """Write a file with the columns and no row after the last sequence."""
    _, conn = small_couchdb

    names, rows = await stage(
        CouchDBSource(conn, shape=CHANGES, secure=False),
        tmp_path,
        {"type": "couchdb", "since": "7-g1AAAA"},
    )

    assert (names, rows) == (CHANGES.columns.names, 0)


async def test_a_changes_read_skips_the_design_documents(small_couchdb, tmp_path):
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
    ("values", "window", "message"),
    [
        pytest.param(
            {"since": "0", "query": "{}"},
            None,
            "one of query and since",
            id="query_and_since",
        ),
        pytest.param(
            {"since": "0"}, Window("1", 1, 2), "one window", id="since_and_window"
        ),
    ],
)
async def test_a_changes_read_refuses_what_it_cannot_combine(
    couchdb, tmp_path, values, window, message
):
    """Raise for a changes read that also asks for a query or for a window."""
    _, conn = couchdb
    source = CouchDBSource(conn, secure=False)

    with pytest.raises(ValueError, match=message):
        await source.stage(
            CLIENT, {"type": "couchdb", **values}, Rows(window=window), tmp_path
        )


async def test_regression_the_last_window_reaches_the_top_of_the_ids(couchdb, tmp_path):
    """Regression: a window that ended at 10^width had an end key before its start."""
    store, conn = couchdb
    store.docs["50000000000"] = {"_id": "50000000000", "_rev": "1-a"}
    store.docs["99999999999"] = {"_id": "99999999999", "_rev": "1-b"}
    source = CouchDBSource(conn, page_size=3000, secure=False)

    await source.stage(
        CLIENT, {"type": "couchdb"}, Rows(window=Window("x", 0, 10**11)), tmp_path
    )
    ids = read_table(tmp_path / "_stage" / "couchdb.parquet")["id"].to_pylist()

    assert (len(ids), ids[-2:]) == (2502, ["50000000000", "99999999999"])


async def test_the_windows_and_the_null_window_read_every_document_once(
    couchdb, tmp_path
):
    """Read the numeric ranges and the null window, and get each document one time."""
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
    source = CouchDBSource(conn, page_size=3000, secure=False)
    read: list[str] = []

    for number, window in enumerate(
        [Window("a", 0, 5 * 10**10), Window("b", 5 * 10**10, 10**11), NULL_WINDOW]
    ):
        folder = tmp_path / str(number)
        folder.mkdir()
        await source.stage(CLIENT, {"type": "couchdb"}, Rows(window=window), folder)
        ids = read_table(folder / "_stage" / "couchdb.parquet")["id"].to_pylist()
        read += [str(key) for key in ids]

    assert sorted(read) == sorted(key for key in store.docs if key != DESIGN)


async def test_a_key_width_cuts_ids_of_mixed_length_by_their_first_digits(
    couchdb, tmp_path
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
    source = CouchDBSource(conn, key_width=3, page_size=3000, secure=False)
    halves: list[list[str]] = []

    for number, window in enumerate([Window("a", 0, 500), Window("b", 500, 1000)]):
        folder = tmp_path / str(number)
        folder.mkdir()
        await source.stage(CLIENT, {"type": "couchdb"}, Rows(window=window), folder)
        ids = read_table(folder / "_stage" / "couchdb.parquet")["id"].to_pylist()
        halves.append([str(key) for key in ids])

    assert halves == [keys[:4], keys[4:]]
    assert "1" not in [seen.get("limit") for _, seen, _ in store.requests]


async def test_a_window_of_dates_is_an_error(couchdb, tmp_path):
    """Raise for a window of dates, because a CouchDB read cuts ids."""
    _, conn = couchdb
    window = Window("2024-01-01", date(2024, 1, 1), date(2024, 1, 2))

    with pytest.raises(ValueError, match="needs numbers"):
        await CouchDBSource(conn, secure=False).stage(
            CLIENT, {"type": "couchdb"}, Rows(window=window), tmp_path
        )
