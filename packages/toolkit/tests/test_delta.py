from pathlib import Path
from threading import Thread

import pytest
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import SchemaMismatchError
from pyarrow import Table
from pyarrow import int64 as arrow_int64
from pyarrow import schema as arrow_schema
from pyarrow import string as arrow_string
from pyarrow.parquet import write_table

from toolkit.delta import (
    fingerprint,
    fingerprint_of,
    known_fingerprints,
    maintain,
    sweep,
    write_window,
)
from toolkit.executor.executor import Executor

SCHEMA = arrow_schema(
    [("id", arrow_int64()), ("nome", arrow_string()), ("batch", arrow_string())]
)


def window_files(directory: Path, key: str, ids: list[int]) -> Path:
    """Write one Parquet file for a window, as the extract step does, and return its folder."""
    rows = [{"id": i, "nome": f"n{i}", "batch": key} for i in ids]
    directory.mkdir(parents=True, exist_ok=True)
    write_table(Table.from_pylist(rows, schema=SCHEMA), directory / "out.parquet")
    return directory


def rows_of(uri: str) -> dict[str, list[int]]:
    """Return the ids of the table, grouped by window."""
    grouped: dict[str, list[int]] = {}
    for row in DeltaTable(uri).to_pyarrow_table().to_pylist():
        grouped.setdefault(row["batch"], []).append(row["id"])
    return {key: sorted(ids) for key, ids in sorted(grouped.items())}


@pytest.fixture
def uri(tmp_path: Path) -> str:
    """Return the path of a Delta table that does not exist yet."""
    return str(tmp_path / "table")


def test_the_first_window_creates_the_table_and_the_ledger(uri, tmp_path):
    """Create the table on the first write, and record the fingerprint of the window."""
    write_window(uri, window_files(tmp_path / "w1", "w1", [1, 2]), "w1", "cfg:w1", 11)

    assert rows_of(uri) == {"w1": [1, 2]}
    assert known_fingerprints(uri, ["cfg:w1", "cfg:w2"]) == {"cfg:w1": 11}


def test_a_table_that_does_not_exist_knows_nothing(uri):
    """Give an empty ledger instead of failing, so the first run loads every window."""
    assert known_fingerprints(uri, ["cfg:w1"]) == {}


def test_a_window_replaces_only_its_own_rows(uri, tmp_path):
    """Swap the rows of one window and leave the others as they are."""
    write_window(uri, window_files(tmp_path / "a", "w1", [1, 2]), "w1", "c:w1", 1)
    write_window(uri, window_files(tmp_path / "b", "w2", [3]), "w2", "c:w2", 2)

    write_window(uri, window_files(tmp_path / "c", "w1", [1, 2, 9]), "w1", "c:w1", 3)

    assert rows_of(uri) == {"w1": [1, 2, 9], "w2": [3]}
    assert known_fingerprints(uri, ["c:w1", "c:w2"]) == {"c:w1": 3, "c:w2": 2}


def test_a_window_with_no_rows_removes_its_old_rows(uri, tmp_path):
    """Expire the rows of a window that is empty in the source now."""
    write_window(uri, window_files(tmp_path / "a", "w1", [1, 2]), "w1", "c:w1", 1)
    write_window(uri, window_files(tmp_path / "b", "w2", [3]), "w2", "c:w2", 2)

    write_window(uri, window_files(tmp_path / "c", "w1", []), "w1", "c:w1", 0)

    assert rows_of(uri) == {"w2": [3]}


def test_a_window_with_other_columns_is_refused_unless_the_schema_is_overwritten(
    uri, tmp_path
):
    """Keep the schema of the table, unless the caller asks to replace it."""
    write_window(uri, window_files(tmp_path / "a", "w1", [1]), "w1", "c:w1", 1)
    narrow = tmp_path / "b"
    narrow.mkdir()
    only_id = Table.from_pylist([{"id": 2, "batch": "w2"}])
    write_table(only_id, narrow / "out.parquet")

    with pytest.raises(SchemaMismatchError):
        write_window(uri, narrow, "w2", "c:w2", 2)

    write_window(uri, narrow, "w2", "c:w2", 2, overwrite_schema=True)

    assert DeltaTable(uri).to_pyarrow_table().column_names == ["id", "batch"]


def test_sweep_removes_the_windows_that_the_config_no_longer_has(uri, tmp_path):
    """Delete every window outside the ones to keep."""
    for key, ids in (("w1", [1]), ("w2", [2]), ("w3", [3])):
        write_window(uri, window_files(tmp_path / key, key, ids), key, f"c:{key}", 1)

    sweep(uri, ["w2", "w3"])

    assert rows_of(uri) == {"w2": [2], "w3": [3]}


def test_sweep_on_a_missing_table_does_nothing(uri):
    """Do not fail when the first run has nothing to sweep."""
    sweep(uri, ["w1"])

    assert not DeltaTable.is_deltatable(uri)


def test_regression_windows_written_at_the_same_time_all_land(uri, tmp_path):
    """Regression: racing creators failed with a changed protocol, and lost windows."""
    errors: list[BaseException] = []

    def put(i: int) -> None:
        """Write one window and keep any error."""
        try:
            key = f"w{i}"
            files = window_files(tmp_path / key, key, [i])
            write_window(uri, files, key, f"c:{key}", i)
        except BaseException as error:
            errors.append(error)

    threads = [Thread(target=put, args=(i,)) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    assert sorted(rows_of(uri)) == [f"w{i}" for i in range(8)]


def test_maintain_writes_a_checkpoint_for_the_readers_of_the_log(uri, tmp_path):
    """Create a checkpoint, so BigQuery does not replay a long JSON log."""
    write_window(uri, window_files(tmp_path / "a", "w1", [1]), "w1", "c:w1", 1)

    maintain(uri)

    log = Path(uri, "_delta_log")
    assert any("checkpoint" in name.name for name in log.iterdir())


def test_the_fingerprint_as_a_number_is_stable():
    """Keep the fingerprint of a known input, so a refactor cannot change every window."""
    assert fingerprint_of(2, "10", "20") == fingerprint_of(2, "10", "20")
    assert fingerprint_of(2, "10", "20") == 0x24133D5443DA121
    assert 0 <= fingerprint_of(0, "0", "0") < 2**63


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
):
    """Give one number for the same rows in any order, and another if the rows differ."""
    executor = Executor(duckdb_memory)
    first = await fingerprint(
        executor, window_files(tmp_path / "a", "w1", [1, 2, 3, 4])
    )

    second = await fingerprint(executor, window_files(tmp_path / "b", "w1", other))

    assert (first == second) is same


async def test_a_changed_value_changes_the_fingerprint(duckdb_memory, tmp_path):
    """Notice a row that keeps its id but changes another column."""
    executor = Executor(duckdb_memory)
    one = window_files(tmp_path / "a", "w1", [1])
    two = tmp_path / "b"
    two.mkdir()
    changed = Table.from_pylist(
        [{"id": 1, "nome": "outro", "batch": "w1"}], schema=SCHEMA
    )
    write_table(changed, two / "out.parquet")

    assert await fingerprint(executor, one) != await fingerprint(executor, two)


def test_regression_a_window_is_written_with_no_storage_options(
    uri, tmp_path, monkeypatch
):
    """Regression: over the S3 API of GCS with HMAC, writers at the same time lost commits.

    `deltalake` must reach `gs://` through its own GCS client and ADC, so the write
    passes no storage options that could switch it to another backend.
    """
    seen: list[dict[str, object]] = []

    def spy(*args, **kwargs):
        """Record the keyword arguments and write as usual."""
        seen.append(kwargs)
        return write_deltalake(*args, **kwargs)

    monkeypatch.setattr("toolkit.delta.write_deltalake", spy)

    write_window(uri, window_files(tmp_path / "a", "w1", [1]), "w1", "c:w1", 1)

    assert seen
    assert all("storage_options" not in call for call in seen)
