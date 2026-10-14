from pathlib import Path

import polars as pl
import pytest
from toolkit import gcs

from prefect_rj_iplanrio.utils import (
    Destination,
    clear_prefix,
    csv_to_parquet,
    publish,
    retries_for,
)

DESTINATION = Destination("b", "ds", "tb")


async def put(store, *keys: str) -> None:
    """Store a small object under each key."""
    for key in keys:
        await store.put_async(key, b"old")


def test_a_destination_names_the_files_and_the_external_table():
    """Name the folder, the URI, the dataset and the table of one BigQuery table."""
    assert DESTINATION.prefix == "staging/ds/tb"
    assert DESTINATION.uri == "gs://b/staging/ds/tb"
    assert DESTINATION.dataset("p") == "p.ds_staging"
    assert DESTINATION.table("p") == "p.ds_staging.tb"


async def test_regression_clearing_a_table_leaves_a_similar_name_alone(store):
    """Regression: the folder of `tb` must not match the files of `tb2`."""
    await put(store, "staging/ds/tb/old.parquet", "staging/ds/tb2/keep.parquet")

    await clear_prefix(store, DESTINATION)

    assert await gcs.list_keys(store, "staging/ds") == ["staging/ds/tb2/keep.parquet"]


@pytest.mark.parametrize(
    ("dump_mode", "exists", "created"),
    [
        pytest.param("overwrite", True, True, id="overwrite_replaces_a_table_that_exists"),
        pytest.param("append", True, False, id="append_keeps_a_table_that_exists"),
        pytest.param("append", False, True, id="append_creates_a_missing_table"),
    ],
)
async def test_the_external_table_is_created_as_the_mode_asks(bigquery, dump_mode, exists, created):
    """Create the dataset, and the table only if the mode asks for it."""
    bigquery.exists = exists

    await publish(DESTINATION, "p", dump_mode, "c")

    table = ("table", "p.ds_staging.tb", "gs://b/staging/ds/tb", "PARQUET", "c")
    assert bigquery.calls[:2] == [("connect", "p"), ("dataset", "p.ds_staging")]
    assert (table in bigquery.calls) is created


async def test_a_csv_folder_becomes_text_parquet_with_the_same_folders(tmp_path: Path):
    """Convert each CSV to Parquet with text columns, and keep the Hive folders."""
    source = tmp_path / "in" / "ano=2024"
    source.mkdir(parents=True)
    (source / "a.csv").write_text("code\n007\n")

    await csv_to_parquet(tmp_path / "in", tmp_path / "out")

    [file] = (tmp_path / "out").rglob("*.parquet")
    assert file.relative_to(tmp_path / "out").as_posix() == "ano=2024/a.parquet"
    assert pl.read_parquet(file).rows() == [("007",)]


@pytest.mark.parametrize(
    ("dump_mode", "retries"),
    [
        pytest.param("overwrite", 2, id="overwrite_runs_again"),
        pytest.param("append", 0, id="append_never_runs_again"),
    ],
)
def test_regression_only_overwrite_runs_a_failed_copy_again(dump_mode, retries):
    """Regression: a second copy on top of a partial first one repeated the rows."""
    assert retries_for(dump_mode) == retries
