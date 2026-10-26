"""Prepare the raw files for the load: records as Parquet files, and the layout as rows."""

from collections.abc import Mapping, Sequence
from pathlib import Path

from polars import DataFrame, String, from_dicts
from pydantic import TypeAdapter

from pipelines.rj_smas__cadunico.helpers.files import unzip_texts, write_text_parquet
from pipelines.rj_smas__cadunico.helpers.layout import parse_layout, read_sheets
from pipelines.rj_smas__cadunico.helpers.partitions import parse_partition, read_header
from pipelines.rj_smas__cadunico.helpers.storage import download_blob
from pipelines.rj_smas__cadunico.types import Partition, Record

RECORDS = TypeAdapter(list[Record])


def to_records(frame: DataFrame) -> list[Record]:
    """Return the rows of a dataframe as text, by column."""
    return RECORDS.validate_python(frame.cast(String).to_dicts())


def prepare_partition(bucket: str, name: str, folder: Path) -> Partition:
    """Download a ZIP of records and write its text files as Parquet files in the folder.

    Every row has the columns of the partition: the layout version of the first line of
    the last text file, and the year, the month and the date of the name of the ZIP.
    Raise `ValueError` if the ZIP has no text file.
    """
    date = parse_partition(name)
    archive = download_blob(bucket, name, folder)
    texts = unzip_texts(archive, folder / "unzipped")

    if not texts:
        raise ValueError(f"The ZIP {name} has no text file")

    layout_version, extracted = read_header(texts[-1])
    year, month, _ = date.split("-")
    constants = {
        "versao_layout_particao": layout_version,
        "ano_particao": str(int(year)),
        "mes_particao": str(int(month)),
        "data_particao": date,
    }
    output = folder / "parquet"
    output.mkdir()
    part = 0

    for text in texts:
        part = write_text_parquet(text, output, constants, part)
        text.unlink()

    return Partition(output, date, layout_version, extracted)


def read_layouts(bucket: str, files: Mapping[str, str], folder: Path) -> list[Record]:
    """Download the layout spreadsheets and return the rows of all of them.

    `files` has the name of each spreadsheet, by version. Each row has the version of
    its file in `versao_layout_particao`. Raise `ValueError` if a spreadsheet has no
    layout.
    """
    rows: list[Record] = []

    for version, name in sorted(files.items()):
        layout = parse_layout(read_sheets(download_blob(bucket, name, folder)), version)

        if not layout:
            raise ValueError(f"The layout {name} has no table of the version {version}")

        rows += [{**row, "versao_layout_particao": version} for row in layout]

    return rows


def write_table(rows: Sequence[Record], folder: Path) -> Path:
    """Write the rows as a Parquet file of text columns in the folder, and return the folder."""
    columns = list(dict.fromkeys(column for row in rows for column in row))
    frame: DataFrame = from_dicts(list(rows), schema=dict.fromkeys(columns, String))
    frame.write_parquet(folder / "data.parquet")

    return folder
