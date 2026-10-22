"""Turn the raw text files of records into Parquet files."""

from pathlib import Path
from zipfile import ZipFile

from pyarrow import Table as ArrowTable
from pyarrow import csv, parquet, string

from pipelines.rj_smas__cadunico.constants import BATCHES_PER_FILE, BLOCK_SIZE, DELIMITER


def unzip_texts(archive: Path, folder: Path) -> list[Path]:
    """Extract a ZIP into the folder and return its text files, by name."""
    with ZipFile(archive) as zipped:
        zipped.extractall(folder)

    return sorted(path for path in folder.iterdir() if path.is_file() and path.suffix.lower() == ".txt")


def write_text_parquet(source: Path, folder: Path, constants: dict[str, str], first_part: int = 0) -> int:
    """Write a text file as Parquet files in the folder and return the next part number.

    A file has the column `text`, which is one line, and the constant columns. The file is
    read in blocks, and `BATCHES_PER_FILE` blocks make one Parquet file, so a file of many
    gigabytes fits in memory and no output is too big. The names are `0000.parquet` and so on.
    """
    reader = csv.open_csv(
        source,
        read_options=csv.ReadOptions(column_names=["text"], block_size=BLOCK_SIZE, encoding="utf8"),
        parse_options=csv.ParseOptions(delimiter=DELIMITER, quote_char=False),
        convert_options=csv.ConvertOptions(column_types={"text": string()}, strings_can_be_null=False),
    )
    part = first_part
    writer: parquet.ParquetWriter | None = None
    written = 0

    for batch in reader:
        data = ArrowTable.from_pydict(
            {
                "text": batch.column("text"),
                **{name: [value] * batch.num_rows for name, value in constants.items()},
            }
        )
        writer = writer or parquet.ParquetWriter(folder / f"{part:04d}.parquet", data.schema)
        writer.write_table(data)
        written += 1

        if written == BATCHES_PER_FILE:
            writer.close()
            writer, written, part = None, 0, part + 1

    if writer is not None:
        writer.close()
        part += 1

    return part
