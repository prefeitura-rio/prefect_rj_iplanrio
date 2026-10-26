from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path
from threading import Thread

import pytest
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import SchemaMismatchError
from pyarrow import Table
from pyarrow import date32 as arrow_date32
from pyarrow import int64 as arrow_int64
from pyarrow import schema as arrow_schema
from pyarrow import string as arrow_string
from pyarrow import timestamp as arrow_timestamp
from pyarrow.parquet import write_table

from toolkit.chunks import LedgerEntry, hash_summary
from toolkit.constants import EXTRACTED_AT_COLUMN
from toolkit.delta import (
    build_merge_predicate,
    compact,
    compute_fingerprint,
    find_layout_problems,
    inspect_table,
    maintain,
    merge_chunk,
    read_known_fingerprints,
    sweep,
    write_chunk,
)
from toolkit.executor.executor import Executor
from toolkit.types import MergeSpec

SCHEMA = arrow_schema(
    [("id", arrow_int64()), ("nome", arrow_string()), ("batch", arrow_string())]
)


def chunk_files(directory: Path, key: str, ids: list[int]) -> Path:
    """Write one Parquet file for a chunk, as the extract step does."""
    rows = [{"id": i, "nome": f"n{i}", "batch": key} for i in ids]
    directory.mkdir(parents=True, exist_ok=True)
    write_table(Table.from_pylist(rows, schema=SCHEMA), directory / "out.parquet")
    return directory


def test_the_first_chunk_creates_the_table_and_the_ledger(
    uri, tmp_path, read_rows
) -> None:
    """Create the table on the first write, and record the fingerprint of the chunk."""
    write_chunk(
        uri, chunk_files(tmp_path / "w1", "w1", [1, 2]), "w1", LedgerEntry("cfg:w1", 11)
    )

    assert read_rows(uri) == {"w1": [1, 2]}
    assert read_known_fingerprints(uri, ["cfg:w1", "cfg:w2"]) == {"cfg:w1": 11}


def test_a_table_that_does_not_exist_knows_nothing(uri) -> None:
    """Give an empty ledger instead of failing, so the first run loads every chunk."""
    assert read_known_fingerprints(uri, ["cfg:w1"]) == {}


def test_a_chunk_replaces_only_its_own_rows(uri, tmp_path, read_rows) -> None:
    """Swap the rows of one chunk and leave the others as they are."""
    write_chunk(
        uri, chunk_files(tmp_path / "a", "w1", [1, 2]), "w1", LedgerEntry("c:w1", 1)
    )
    write_chunk(
        uri, chunk_files(tmp_path / "b", "w2", [3]), "w2", LedgerEntry("c:w2", 2)
    )

    write_chunk(
        uri, chunk_files(tmp_path / "c", "w1", [1, 2, 9]), "w1", LedgerEntry("c:w1", 3)
    )

    assert read_rows(uri) == {"w1": [1, 2, 9], "w2": [3]}
    assert read_known_fingerprints(uri, ["c:w1", "c:w2"]) == {"c:w1": 3, "c:w2": 2}


def test_a_chunk_with_no_rows_removes_its_old_rows(uri, tmp_path, read_rows) -> None:
    """Expire the rows of a chunk that is empty in the source now."""
    write_chunk(
        uri, chunk_files(tmp_path / "a", "w1", [1, 2]), "w1", LedgerEntry("c:w1", 1)
    )
    write_chunk(
        uri, chunk_files(tmp_path / "b", "w2", [3]), "w2", LedgerEntry("c:w2", 2)
    )

    write_chunk(
        uri, chunk_files(tmp_path / "c", "w1", []), "w1", LedgerEntry("c:w1", 0)
    )

    assert read_rows(uri) == {"w2": [3]}


def test_a_chunk_with_other_columns_is_refused_unless_the_schema_is_overwritten(
    uri, tmp_path
) -> None:
    """Keep the schema of the table, unless the caller asks to replace it."""
    write_chunk(
        uri, chunk_files(tmp_path / "a", "w1", [1]), "w1", LedgerEntry("c:w1", 1)
    )
    narrow = tmp_path / "b"
    narrow.mkdir()
    only_id = Table.from_pylist([{"id": 2, "batch": "w2"}])
    write_table(only_id, narrow / "out.parquet")

    with pytest.raises(SchemaMismatchError):
        write_chunk(uri, narrow, "w2", LedgerEntry("c:w2", 2))

    write_chunk(uri, narrow, "w2", LedgerEntry("c:w2", 2), {"overwrite_schema": True})

    assert DeltaTable(uri).to_pyarrow_table().column_names == ["id", "batch"]


def test_sweep_removes_the_chunks_that_the_config_no_longer_has(
    uri, tmp_path, read_rows
) -> None:
    """Delete every chunk outside the ones to keep."""
    for key, ids in (("w1", [1]), ("w2", [2]), ("w3", [3])):
        write_chunk(
            uri, chunk_files(tmp_path / key, key, ids), key, LedgerEntry(f"c:{key}", 1)
        )

    sweep(uri, ["w2", "w3"])

    assert read_rows(uri) == {"w2": [2], "w3": [3]}


def test_sweep_on_a_missing_table_does_nothing(uri) -> None:
    """Do not fail when the first run has nothing to sweep."""
    sweep(uri, ["w1"])

    assert not DeltaTable.is_deltatable(uri)


def test_regression_chunks_written_at_the_same_time_all_land(
    uri, tmp_path, read_rows
) -> None:
    """Regression: racing creators failed with a changed protocol, and lost chunks."""
    errors: list[BaseException] = []

    def put(i: int) -> None:
        """Write one chunk and keep any error."""
        try:
            key = f"w{i}"
            files = chunk_files(tmp_path / key, key, [i])
            write_chunk(uri, files, key, LedgerEntry(f"c:{key}", i))
        except BaseException as error:
            errors.append(error)

    threads = [Thread(target=put, args=(i,)) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    assert sorted(read_rows(uri)) == [f"w{i}" for i in range(8)]


def test_maintain_writes_a_checkpoint_for_the_readers_of_the_log(uri, tmp_path) -> None:
    """Create a checkpoint, so BigQuery does not replay a long JSON log."""
    write_chunk(
        uri, chunk_files(tmp_path / "a", "w1", [1]), "w1", LedgerEntry("c:w1", 1)
    )

    maintain(uri)

    log = Path(uri, "_delta_log")
    assert any("checkpoint" in name.name for name in log.iterdir())


@pytest.mark.parametrize(
    ("commits", "checkpoint"),
    [
        pytest.param(99, False, id="no_checkpoint_before_the_interval"),
        pytest.param(
            101, True, id="a_checkpoint_when_the_version_reaches_the_interval"
        ),
    ],
)
def test_a_dump_of_many_chunks_checkpoints_the_log_on_its_own(
    uri, tmp_path, commits, checkpoint
) -> None:
    """Write a checkpoint every 100 commits, so a write never replays a long log."""
    for number in range(commits):
        key = f"w{number}"
        write_chunk(
            uri,
            chunk_files(tmp_path / key, key, [number]),
            key,
            LedgerEntry(f"c:{key}", 1),
            {"append": True},
        )

    log = Path(uri, "_delta_log")

    assert any("checkpoint" in name.name for name in log.iterdir()) is checkpoint
    assert DeltaTable(uri).to_pyarrow_table().num_rows == commits


@pytest.mark.parametrize(
    ("keys", "expected"),
    [
        pytest.param(
            ["2024-03-01", "2024-03-02", "null"], [], id="dates_and_null_are_one_layout"
        ),
        pytest.param(["000000000000", "000000000001"], [], id="numbers_are_one_layout"),
        pytest.param(["all"], [], id="all_alone_is_one_layout"),
        pytest.param(
            ["all", "2024-03-01"], ["all next to chunk keys"], id="all_next_to_dates"
        ),
        pytest.param(
            ["all", "null"], ["all next to chunk keys"], id="all_next_to_null"
        ),
        pytest.param(
            ["2024-03-01", "000000000001"],
            ["dates next to numbers"],
            id="dates_next_to_numbers",
        ),
    ],
)
def test_the_layout_problems_follow_the_chunk_keys(keys, expected) -> None:
    """Flag the keys that show two cuts of the same table, which leave rows twice."""
    assert find_layout_problems(keys) == expected


def test_inspect_reports_the_version_the_files_and_the_chunk_keys(
    uri, tmp_path
) -> None:
    """Read the state of the table from its log."""
    write_chunk(
        uri, chunk_files(tmp_path / "a", "w1", [1, 2]), "w1", LedgerEntry("c:w1", 1)
    )
    write_chunk(
        uri, chunk_files(tmp_path / "b", "w2", [3]), "w2", LedgerEntry("c:w2", 2)
    )

    report = inspect_table(uri)

    assert (report["version"], report["files"], report["keys"]) == (1, 2, ["w1", "w2"])
    assert report["bytes"] > 0
    assert report["last_commit"].tzinfo is UTC


def test_compact_lowers_the_files_that_inspect_counts_and_keeps_the_rows(
    uri, tmp_path, read_rows
) -> None:
    """Merge the small files of a chunk into one, with the same rows."""
    for number in range(3):
        directory = chunk_files(tmp_path / f"a{number}", "w1", [number])
        write_chunk(
            uri, directory, "w1", LedgerEntry(f"c:{number}", number), {"append": True}
        )
    before = inspect_table(uri)["files"]

    compact(uri)

    assert (before, inspect_table(uri)["files"]) == (3, 1)
    assert read_rows(uri) == {"w1": [0, 1, 2]}


def test_the_fingerprint_as_a_number_is_stable() -> None:
    """Keep the fingerprint of a known input, so a refactor cannot change it."""
    assert hash_summary(2, "10", "20") == hash_summary(2, "10", "20")
    assert hash_summary(2, "10", "20") == 0x24133D5443DA121
    assert 0 <= hash_summary(0, "0", "0") < 2**63


@pytest.mark.parametrize(
    ("other", "same"),
    [
        pytest.param([1, 2, 3], False, id="a_row_removed"),
        pytest.param([1, 2, 3, 4, 5], False, id="a_row_added"),
        pytest.param([4, 3, 2, 1], True, id="the_same_rows_in_another_order"),
        pytest.param([1, 2, 3, 3], False, id="a_row_repeated"),
    ],
)
async def test_the_fingerprint_follows_the_content_not_the_order(
    duckdb_memory, tmp_path, other, same
) -> None:
    """Give one number for the same rows in any order, and another for other rows."""
    executor = Executor(duckdb_memory)
    first = await compute_fingerprint(
        executor, chunk_files(tmp_path / "a", "w1", [1, 2, 3, 4])
    )

    second = await compute_fingerprint(
        executor, chunk_files(tmp_path / "b", "w1", other)
    )

    assert (first == second) is same


async def test_a_changed_value_changes_the_fingerprint(duckdb_memory, tmp_path) -> None:
    """Notice a row that keeps its id but changes another column."""
    executor = Executor(duckdb_memory)
    one = chunk_files(tmp_path / "a", "w1", [1])
    two = tmp_path / "b"
    two.mkdir()
    changed = Table.from_pylist(
        [{"id": 1, "nome": "outro", "batch": "w1"}], schema=SCHEMA
    )
    write_table(changed, two / "out.parquet")

    assert await compute_fingerprint(executor, one) != await compute_fingerprint(
        executor, two
    )


def test_regression_a_chunk_is_written_with_no_storage_options(
    uri, tmp_path, monkeypatch
) -> None:
    """Regression: writers over the S3 API of GCS lost commits.

    `deltalake` must reach `gs://` with its own GCS client and ADC, so a write passes
    no storage options that could switch it to another backend.
    """
    seen: list[dict[str, object]] = []

    def spy(*args, **kwargs):
        """Record the keyword arguments and write as usual."""
        seen.append(kwargs)
        return write_deltalake(*args, **kwargs)

    monkeypatch.setattr("toolkit.delta.write_deltalake", spy)

    write_chunk(
        uri, chunk_files(tmp_path / "a", "w1", [1]), "w1", LedgerEntry("c:w1", 1)
    )

    assert seen
    assert all("storage_options" not in call for call in seen)


def test_append_adds_the_rows_and_keeps_the_ones_of_the_chunk(
    uri, tmp_path, read_rows
) -> None:
    """Add a second capture in the same chunk, with no row lost."""
    write_chunk(
        uri, chunk_files(tmp_path / "a", "w", [1, 2]), "w", LedgerEntry("c:1", 1)
    )
    write_chunk(
        uri,
        chunk_files(tmp_path / "b", "w", [3]),
        "w",
        LedgerEntry("c:2", 2),
        {"append": True},
    )

    assert read_rows(uri) == {"w": [1, 2, 3]}
    assert read_known_fingerprints(uri, ["c:1", "c:2"]) == {"c:1": 1, "c:2": 2}


def test_compact_merges_the_files_of_a_chunk_and_loses_no_row(
    uri, tmp_path, read_rows
) -> None:
    """Leave one file where there were three, with the same rows."""
    for number in range(3):
        write_chunk(
            uri,
            chunk_files(tmp_path / f"w{number}", "w", [number]),
            "w",
            LedgerEntry(f"c:{number}", number),
            {"append": True},
        )
    assert len(DeltaTable(uri).file_uris()) == 3

    compact(uri)

    assert len(DeltaTable(uri).file_uris()) == 1
    assert read_rows(uri) == {"w": [0, 1, 2]}


def test_a_chunk_written_with_a_time_has_it_on_every_row(uri, tmp_path) -> None:
    """Add `_prefect_extracted_at` as a time stamp in UTC, the same on every row."""
    moment = datetime(2024, 3, 10, 12, 0, tzinfo=UTC)

    write_chunk(
        uri,
        chunk_files(tmp_path / "w", "w", [1, 2]),
        "w",
        LedgerEntry("c:1", 1),
        {"extracted_at": moment},
    )

    table = DeltaTable(uri).to_pyarrow_table()
    assert str(table.schema.field(EXTRACTED_AT_COLUMN).type) == "timestamp[us, tz=UTC]"
    assert set(table.column(EXTRACTED_AT_COLUMN).to_pylist()) == {moment}


def test_a_chunk_written_without_a_time_has_no_column(uri, tmp_path) -> None:
    """Leave the table as the files are, unless a time is asked for."""
    write_chunk(uri, chunk_files(tmp_path / "w", "w", [1]), "w", LedgerEntry("c:1", 1))

    assert EXTRACTED_AT_COLUMN not in DeltaTable(uri).to_pyarrow_table().column_names


def merge_files(directory: Path, rows: dict[int, str]) -> Path:
    """Write the rows `id -> nome` as one Parquet file in a chunk named `run`."""
    directory.mkdir(parents=True, exist_ok=True)
    data = [{"id": i, "nome": nome, "batch": "run"} for i, nome in rows.items()]
    write_table(Table.from_pylist(data, schema=SCHEMA), directory / "out.parquet")
    return directory


def read_names(uri: str) -> dict[int, str]:
    """Return the name of each id in the table."""
    return {
        row["id"]: row["nome"] for row in DeltaTable(uri).to_pyarrow_table().to_pylist()
    }


def test_the_first_merge_creates_the_table_and_records_the_run(uri, tmp_path) -> None:
    """Create the table with the rows, and the ledger entry in the same commit."""
    merge_chunk(
        uri,
        merge_files(tmp_path / "a", {1: "a", 2: "b"}),
        MergeSpec(key=["id"]),
        LedgerEntry("run:1", 1),
    )

    assert read_names(uri) == {1: "a", 2: "b"}
    assert read_known_fingerprints(uri, ["run:1"]) == {"run:1": 1}


@pytest.mark.parametrize(
    ("update", "expected"),
    [
        pytest.param(
            True,
            {1: "a", 2: "changed", 3: "c"},
            id="an_update_changes_the_row_and_adds_the_new_one",
        ),
        pytest.param(
            False,
            {1: "a", 2: "b", 3: "c"},
            id="insert_only_keeps_the_old_row_and_adds_the_new_one",
        ),
    ],
)
def test_a_merge_adds_new_keys_and_updates_only_when_asked(
    uri, tmp_path, update, expected
) -> None:
    """Add key 3, and change key 2 only with `update`."""
    spec = MergeSpec(key=["id"], update=update)
    merge_chunk(
        uri,
        merge_files(tmp_path / "a", {1: "a", 2: "b"}),
        spec,
        LedgerEntry("run:1", 1),
    )

    merge_chunk(
        uri,
        merge_files(tmp_path / "b", {2: "changed", 3: "c"}),
        spec,
        LedgerEntry("run:2", 1),
    )

    assert read_names(uri) == expected


def test_the_same_data_merged_again_adds_no_row_and_rewrites_no_file(
    uri, tmp_path
) -> None:
    """Make a rerun, or an overlapping window, change nothing."""
    spec = MergeSpec(key=["id"])
    merge_chunk(
        uri,
        merge_files(tmp_path / "a", {1: "a", 2: "b"}),
        spec,
        LedgerEntry("run:1", 1),
    )
    files = set(DeltaTable(uri).file_uris())

    merge_chunk(
        uri,
        merge_files(tmp_path / "b", {1: "a", 2: "b"}),
        spec,
        LedgerEntry("run:2", 1),
    )

    assert read_names(uri) == {1: "a", 2: "b"}
    assert set(DeltaTable(uri).file_uris()) == files


def test_a_merge_with_a_repeated_key_in_the_new_data_is_refused(uri, tmp_path) -> None:
    """Refuse two rows with one key: Delta would insert both, or fail an update."""
    folder = tmp_path / "dup"
    folder.mkdir()
    rows = [
        {"id": 1, "nome": "x", "batch": "run"},
        {"id": 1, "nome": "y", "batch": "run"},
    ]
    write_table(Table.from_pylist(rows, schema=SCHEMA), folder / "out.parquet")

    with pytest.raises(ValueError, match="key"):
        merge_chunk(uri, folder, MergeSpec(key=["id"]), LedgerEntry("run:1", 1))

    assert not DeltaTable.is_deltatable(uri)


def test_a_merge_with_no_rows_changes_nothing(uri, tmp_path) -> None:
    """Leave the table and its ledger alone when the run found nothing."""
    merge_chunk(
        uri,
        merge_files(tmp_path / "a", {1: "a"}),
        MergeSpec(key=["id"]),
        LedgerEntry("run:1", 1),
    )
    version = DeltaTable(uri).version()

    merge_chunk(
        uri,
        merge_files(tmp_path / "empty", {}),
        MergeSpec(key=["id"]),
        LedgerEntry("run:2", 1),
    )

    assert DeltaTable(uri).version() == version
    assert read_known_fingerprints(uri, ["run:2"]) == {}


def test_a_merge_with_a_time_stamps_new_and_changed_rows_only(uri, tmp_path) -> None:
    """Keep the old time on a row that did not change."""
    first = datetime(2024, 3, 10, 12, 0, tzinfo=UTC)
    second = datetime(2024, 3, 10, 13, 0, tzinfo=UTC)
    merge_chunk(
        uri,
        merge_files(tmp_path / "a", {1: "a", 2: "b"}),
        MergeSpec(key=["id"]),
        LedgerEntry("run:1", 1),
        first,
    )

    merge_chunk(
        uri,
        merge_files(tmp_path / "b", {1: "a", 2: "changed", 3: "c"}),
        MergeSpec(key=["id"]),
        LedgerEntry("run:2", 1),
        second,
    )

    stamps = {
        row["id"]: row[EXTRACTED_AT_COLUMN]
        for row in DeltaTable(uri).to_pyarrow_table().to_pylist()
    }
    assert stamps == {1: first, 2: second, 3: second}


def test_a_merge_with_other_columns_is_refused(uri, tmp_path) -> None:
    """Keep the schema rule of an append: other columns are an error."""
    merge_chunk(
        uri,
        merge_files(tmp_path / "a", {1: "a"}),
        MergeSpec(key=["id"]),
        LedgerEntry("run:1", 1),
    )
    folder = tmp_path / "wide"
    folder.mkdir()
    wide = arrow_schema(
        [("id", arrow_int64()), ("extra", arrow_string()), ("batch", arrow_string())]
    )
    write_table(
        Table.from_pylist([{"id": 2, "extra": "x", "batch": "run"}], schema=wide),
        folder / "out.parquet",
    )

    with pytest.raises(SchemaMismatchError, match="extra"):
        merge_chunk(uri, folder, MergeSpec(key=["id"]), LedgerEntry("run:2", 1))

    assert read_names(uri) == {1: "a"}


@pytest.mark.parametrize(
    ("prune", "scanned"),
    [
        pytest.param(None, 3, id="without_a_prune_column_every_file_is_read"),
        pytest.param("nome", 1, id="with_a_prune_column_only_the_recent_file_is_read"),
    ],
)
def test_a_prune_column_lowers_the_files_that_a_merge_reads(
    uri, tmp_path, prune, scanned
) -> None:
    """Read one file instead of three, when the key sits in the last one."""
    for number, nome in enumerate("abc", start=1):
        merge_chunk(
            uri,
            merge_files(tmp_path / nome, {number: nome}),
            MergeSpec(key=["id"]),
            LedgerEntry(f"run:{number}", 1),
        )
    spec = MergeSpec(key=["id"], prune=prune) if prune else MergeSpec(key=["id"])

    report = merge_chunk(
        uri, merge_files(tmp_path / "again", {3: "c"}), spec, LedgerEntry("run:4", 1)
    )

    assert report == {"rows": 1, "inserted": 0, "updated": 0, "scanned": scanned}


def test_a_merge_reports_the_rows_that_it_added_and_changed(uri, tmp_path) -> None:
    """Count the rows of each outcome, for the log and for the caller."""
    spec = MergeSpec(key=["id"])
    first = merge_chunk(
        uri,
        merge_files(tmp_path / "a", {1: "a", 2: "b"}),
        spec,
        LedgerEntry("run:1", 1),
    )

    second = merge_chunk(
        uri,
        merge_files(tmp_path / "b", {2: "changed", 3: "c"}),
        spec,
        LedgerEntry("run:2", 1),
    )

    assert first == {"rows": 2, "inserted": 2, "updated": 0, "scanned": 0}
    assert (second["rows"], second["inserted"], second["updated"]) == (2, 1, 1)


def test_a_merge_prunes_by_a_column_with_a_time_zone(uri, tmp_path) -> None:
    """Compare the lowest time of the new data with a UTC column, with no duplicate."""
    zoned = arrow_schema(
        [
            ("id", arrow_int64()),
            ("at", arrow_timestamp("us", "UTC")),
            ("batch", arrow_string()),
        ]
    )
    folder = tmp_path / "zoned"
    folder.mkdir()
    rows = [
        {"id": i, "at": datetime(2026, 10, 22, i, tzinfo=UTC), "batch": "run"}
        for i in (1, 2)
    ]
    write_table(Table.from_pylist(rows, schema=zoned), folder / "out.parquet")
    spec = MergeSpec(key=["id"], prune="at")
    merge_chunk(uri, folder, spec, LedgerEntry("run:1", 1))

    report = merge_chunk(uri, folder, spec, LedgerEntry("run:2", 1))

    assert report["inserted"] == 0
    assert DeltaTable(uri).to_pyarrow_table().num_rows == 2


@pytest.mark.parametrize(
    ("spec", "low", "expected"),
    [
        pytest.param(MergeSpec(key=["id"]), None, 't."id" = s."id"', id="one_key"),
        pytest.param(
            MergeSpec(key=["a", "b"]),
            None,
            't."a" = s."a" AND t."b" = s."b"',
            id="two_keys",
        ),
        pytest.param(
            MergeSpec(key=["id"], prune="day"),
            "2026-10-22",
            't."id" = s."id" AND t."day" >= \'2026-10-22\'',
            id="a_text_prune",
        ),
        pytest.param(
            MergeSpec(key=["id"], prune="day"),
            date(2026, 10, 22),
            't."id" = s."id" AND t."day" >= CAST(\'2026-10-22\' AS DATE)',
            id="a_date_prune",
        ),
        pytest.param(
            MergeSpec(key=["id"], prune="n"),
            5,
            't."id" = s."id" AND t."n" >= 5',
            id="a_number_prune",
        ),
        pytest.param(
            MergeSpec(key=["id"], prune="at"),
            datetime(2026, 10, 22, 0, 30, tzinfo=timezone(timedelta(hours=-3))),
            't."id" = s."id" AND t."at" >= CAST(\'2026-10-22 00:30:00-03:00\' AS TIMESTAMP)',
            id="a_time_with_an_offset",
        ),
        pytest.param(
            MergeSpec(key=["id"], prune="day"),
            "o'brien",
            't."id" = s."id" AND t."day" >= \'o\'\'brien\'',
            id="a_quote_in_the_value",
        ),
    ],
)
def test_the_merge_predicate_matches_the_key_and_prunes_old_files(
    spec, low, expected
) -> None:
    """Join the key columns, and bound the scan by the lowest prune value."""
    assert build_merge_predicate(spec, low) == expected


def test_regression_a_merge_updates_a_row_that_differs_in_the_last_of_several_columns(
    uri, tmp_path
) -> None:
    """Regression: two columns besides the key must not mix the terms of the test."""
    wide = arrow_schema(
        [
            ("id", arrow_int64()),
            ("nome", arrow_string()),
            ("dia", arrow_date32()),
            ("batch", arrow_string()),
        ]
    )

    def put(folder: str, rows: list[tuple[int, str, date]]) -> Path:
        path = tmp_path / folder
        path.mkdir()
        data = [{"id": i, "nome": n, "dia": d, "batch": "run"} for i, n, d in rows]
        write_table(Table.from_pylist(data, schema=wide), path / "out.parquet")
        return path

    spec = MergeSpec(key=["id"])
    merge_chunk(
        uri,
        put("a", [(1, "a", date(2026, 10, 22)), (2, "b", date(2026, 10, 22))]),
        spec,
        LedgerEntry("run:1", 1),
    )
    files = set(DeltaTable(uri).file_uris())

    merge_chunk(
        uri,
        put("b", [(1, "a", date(2026, 10, 22)), (2, "b", date(2026, 10, 23))]),
        spec,
        LedgerEntry("run:2", 1),
    )

    days = {
        row["id"]: row["dia"] for row in DeltaTable(uri).to_pyarrow_table().to_pylist()
    }
    assert days == {1: date(2026, 10, 22), 2: date(2026, 10, 23)}
    assert set(DeltaTable(uri).file_uris()) != files


@pytest.mark.parametrize(
    ("key", "rows"),
    [
        pytest.param(["id"], [(None, "a")], id="a_null_in_the_only_key_column"),
        pytest.param(["id", "nome"], [(1, None)], id="a_null_in_the_second_key_column"),
    ],
)
def test_a_merge_with_a_null_in_the_key_is_refused(uri, tmp_path, key, rows) -> None:
    """Refuse a row that no key can match: it would be added again on every run."""
    folder = tmp_path / "nulls"
    folder.mkdir()
    data = [{"id": i, "nome": n, "batch": "run"} for i, n in rows]
    write_table(Table.from_pylist(data, schema=SCHEMA), folder / "out.parquet")

    with pytest.raises(ValueError, match="NULL"):
        merge_chunk(uri, folder, MergeSpec(key=key), LedgerEntry("run:1", 1))

    assert not DeltaTable.is_deltatable(uri)
