from collections.abc import Callable
from pathlib import Path

import pytest
from polars import read_parquet

from pipelines.rj_smas__cadunico.helpers.prepare import prepare_partition, to_records, write_table
from pipelines.rj_smas__cadunico.types import Record

HEADER = " " * 69 + "7.01 " + " " * 8 + "31012024" + " " * 20


def test_a_zip_becomes_parquet_files_with_the_partition_columns(
    serve_archive: Callable[[Path], None], make_archive: Callable[[str, dict[str, str]], Path], tmp_path: Path
) -> None:
    """Take the layout version from the header, and the date from the ZIP name."""
    archive = make_archive("base_20240131.zip", {"a.txt": f"{HEADER}\nsecond\n"})
    serve_archive(archive)

    partition = prepare_partition("raw", "dir/base_20240131.zip", tmp_path / "work")

    frame = read_parquet(partition.folder / "*.parquet")
    assert (partition.date, partition.layout_version, partition.extracted) == ("2024-01-31", "701", "2024-01-31")
    assert frame["text"].to_list() == [HEADER, "second"]
    assert frame.row(0, named=True)["ano_particao"] == "2024"
    assert frame.row(0, named=True)["mes_particao"] == "1"
    assert frame.row(0, named=True)["versao_layout_particao"] == "701"


def test_a_zip_with_no_text_file_is_refused(
    serve_archive: Callable[[Path], None], make_archive: Callable[[str, dict[str, str]], Path], tmp_path: Path
) -> None:
    """Raise for a ZIP that has no text file."""
    archive = make_archive("base_20240131.zip", {"a.csv": "x"})
    serve_archive(archive)

    with pytest.raises(ValueError, match="no text file"):
        prepare_partition("raw", "dir/base_20240131.zip", tmp_path / "work")


def test_the_rows_are_written_and_read_back_as_text(tmp_path: Path) -> None:
    """Keep every column as text, and keep a missing value as nothing."""
    rows: list[Record] = [{"a": "1", "b": None}, {"a": "2", "b": "x\ny"}]

    folder = write_table(rows, tmp_path)

    assert to_records(read_parquet(folder / "data.parquet")) == rows
