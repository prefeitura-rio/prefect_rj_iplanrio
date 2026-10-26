from collections.abc import Callable
from pathlib import Path

import pytest
from polars import read_parquet

from pipelines.rj_smas__cadunico.helpers import files
from pipelines.rj_smas__cadunico.helpers.files import unzip_texts, write_text_parquet


def test_the_zip_gives_its_text_files_by_name(
    make_archive: Callable[[str, dict[str, str]], Path], tmp_path: Path
) -> None:
    """Extract the text files, in any case of the extension, and leave out the others."""
    archive = make_archive("a.zip", {"b.TXT": "1", "a.txt": "2", "c.csv": "3"})

    texts = unzip_texts(archive, tmp_path / "out")

    assert [path.name for path in texts] == ["a.txt", "b.TXT"]


@pytest.mark.parametrize(
    ("batches_per_file", "first_part", "names"),
    [
        pytest.param(1000, 0, ["0000.parquet"], id="one_file_when_the_limit_is_far"),
        pytest.param(1, 3, ["0003.parquet", "0004.parquet", "0005.parquet"], id="a_file_for_each_batch_from_part_3"),
    ],
)
def test_the_text_becomes_parquet_files_with_the_constant_columns(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    batches_per_file: int,
    first_part: int,
    names: list[str],
) -> None:
    """Write a file for every `BATCHES_PER_FILE` batches, with every line and the constants."""
    monkeypatch.setattr(files, "BATCHES_PER_FILE", batches_per_file)
    monkeypatch.setattr(files, "BLOCK_SIZE", 2048)
    lines = [f"{number:04d}" + "x" * 96 for number in range(60)] if batches_per_file == 1 else ["a", "b"]
    source = tmp_path / "base.txt"
    source.write_text("\n".join(lines) + "\n", encoding="utf-8")
    target = tmp_path / "out"
    target.mkdir()

    next_part = write_text_parquet(source, target, {"data_particao": "2024-01-31"}, first_part)

    written = sorted(path.name for path in target.iterdir())
    frame = read_parquet(target / "*.parquet")
    assert written == names
    assert next_part == first_part + len(written)
    assert frame["text"].to_list() == lines
    assert frame["data_particao"].unique().to_list() == ["2024-01-31"]
