"""Read the layout spreadsheets of CadÚnico into rows."""

from collections.abc import Sequence
from pathlib import Path
from re import search

from openpyxl import load_workbook
from unidecode import unidecode
from xlrd import open_workbook

from pipelines.rj_smas__cadunico.constants import (
    BLOB_VERSION_MARK,
    LAYOUT_TITLE,
    TABLE_FIELDS,
    UNNAMED_COLUMN,
    VERSION_DIGITS,
)
from pipelines.rj_smas__cadunico.types import Cells, Record

HEADER_MARK = "reg"


def parse_blob_version(name: str) -> str:
    """Return the version in the name of a layout file, with four digits.

    ```python
    parse_blob_version("layout/BMM_V7_01.xlsx")  # "0701"
    parse_blob_version("layout/BMM_V10_1.xlsx")  # "1001"
    ```
    """
    version = name.split(BLOB_VERSION_MARK)[1].split(".", maxsplit=1)[0].replace("_", "")

    return version.zfill(VERSION_DIGITS)


def read_title_version(title: str) -> str | None:
    """Return the version `7.01` in the title of a table, or nothing if it has none."""
    found = search(r"\d+\.\d+", title)

    return found.group() if found else None


def name_version(version: str) -> str:
    """Return the version of a file name as the title of a table writes it.

    ```python
    name_version("0701")  # "7.01"
    name_version("1001")  # "10.01"
    ```
    """
    return f"{int(version[:2])}.{version[2:]}"


def text_of(value: object) -> str | None:
    """Return a cell as text, or nothing if it is empty. A whole number has no decimals."""
    if value is None or value == "":
        return None

    if isinstance(value, float) and value.is_integer():
        return str(int(value))

    return str(value)


def read_sheets(path: Path) -> dict[str, Cells]:
    """Return the cells of each sheet of a spreadsheet, as text, by sheet name."""
    if path.suffix.lower() == ".xls":
        book = open_workbook(str(path))

        return {
            sheet.name: [[text_of(cell) for cell in sheet.row_values(row)] for row in range(sheet.nrows)]
            for sheet in book.sheets()
        }

    workbook = load_workbook(path, read_only=True, data_only=True)

    return {
        sheet.title: [[text_of(cell) for cell in row] for row in sheet.iter_rows(values_only=True)]
        for sheet in workbook.worksheets
    }


def join_notes(rows: list[Record]) -> list[Record]:
    """Move the description of a row with no `reg` to the row above that has one, and drop the rows with no position."""
    last: Record | None = None

    for row in rows:
        if row["reg"] is None:
            if last is not None and row["descricao"] is not None:
                last["descricao"] = f"{last['descricao'] or ''}    \n{row['descricao'].strip()}"
        else:
            last = row

    return [row for row in rows if row["posicao"] is not None]


def read_table(cells: Cells, title_row: int, title_column: int, title: str, sheet: str) -> list[Record]:
    """Return the rows of the table that starts below a title cell.

    The table has the columns in `TABLE_FIELDS`, from the column of the title. The row
    under the title is the header, and it goes away.
    """
    width = len(TABLE_FIELDS)
    rows = [(row[title_column : title_column + width] + [None] * width)[:width] for row in cells[title_row + 1 :]]

    if not rows:
        return []

    header_note = (rows[0][-1] or "").lower()
    version = read_title_version(title)
    records: list[Record] = []

    for row in rows[1:]:
        fields = dict(zip(TABLE_FIELDS, row, strict=True))

        if header_note == HEADER_MARK:
            fields["observacoes"] = None

        records.append({"table": sheet, "version": version, **fields})

    return join_notes(records)


def read_tables(cells: Cells, sheet: str) -> list[Record]:
    """Return the rows of every table of a sheet, from each cell whose text has the layout title."""
    records: list[Record] = []

    for row_number, row in enumerate(cells):
        for column_number, value in enumerate(row):
            if value is not None and LAYOUT_TITLE in value:
                records += read_table(cells, row_number, column_number, value, sheet)

    return records


def clean_name(name: str | None) -> str:
    """Return a column name without accents, in lower case, with `_` where it had a space, `-` or `/`."""
    text = unidecode(name or UNNAMED_COLUMN)

    return text.replace("-", "_").replace(" ", "_").replace("/", "_").lower().strip()


def name_columns(records: Sequence[Record]) -> list[Record]:
    """Add the `column` of each row: its clean name, with `_2`, `_3` for a repeat in its table and version."""
    counts: dict[tuple[str | None, str | None], dict[str, int]] = {}
    named: list[Record] = []

    for record in records:
        name = clean_name(record["arquivo_base_versao_7"])
        seen = counts.setdefault((record["reg"], record["version"]), {})
        seen[name] = seen.get(name, 0) + 1
        named.append({**record, "column": name if seen[name] == 1 else f"{name}_{seen[name]}"})

    return named


def parse_layout(cells_by_sheet: dict[str, Cells], version: str) -> list[Record]:
    """Return the rows of the layout of one version, from the sheets of its spreadsheet.

    A row is a field of a type of record. It has the `column` that names the field.
    """
    records = [record for sheet, cells in cells_by_sheet.items() for record in read_tables(cells, sheet)]
    wanted = name_version(version)

    return name_columns([record for record in records if record["version"] == wanted])
