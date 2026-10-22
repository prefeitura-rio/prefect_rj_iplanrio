"""Helpers of rj_cor__precipitacao_alertario."""

from collections.abc import Mapping, Sequence
from datetime import datetime

from bs4 import BeautifulSoup, Tag

from pipelines.rj_cor__precipitacao_alertario.constants import MISSING, TIME_FORMAT


def read_cells(row: Tag, name: str) -> list[str]:
    """Return the text of the cells of a table row, without spaces around."""
    return [cell.get_text(strip=True) for cell in row.find_all(name)]


def read_table(table: Tag) -> list[dict[str, str]]:
    """Return the rows of an HTML table, by the name of the last header row.

    A header with two levels keeps the one nearest to the data. The numbers of the
    page have a decimal comma, and the rows come as text with a decimal point.
    """
    rows = table.find_all("tr")
    headers = [read_cells(row, "th") for row in rows if row.find("th") and not row.find("td")]
    names = headers[-1] if headers else []

    return [
        dict(zip(names, (cell.replace(",", ".") for cell in read_cells(row, "td")), strict=False))
        for row in rows
        if row.find("td")
    ]


def read_tables(html: str) -> list[list[dict[str, str]]]:
    """Return every table of the page as rows."""
    return [read_table(table) for table in BeautifulSoup(html, "html.parser").find_all("table")]


def build_rows(rows: Sequence[Mapping[str, str]], columns: Mapping[str, str]) -> list[dict[str, str | None]]:
    """Return the rows with the columns of the table, in order, and the time in text.

    A repeated station and time keeps the first row. A column that the page lacks is
    empty, and so is a reading of `ND` or `-`.
    """
    built: dict[tuple[str, str], dict[str, str | None]] = {}

    for row in rows:
        measured = datetime.strptime(row["Hora Leitura"], TIME_FORMAT)  # noqa: DTZ007  # the page has no time zone
        key = (row["N°"], measured.isoformat())

        if key in built:
            continue

        values = {
            name: (None if row.get(source) in (None, *MISSING) else row[source]) for source, name in columns.items()
        }
        built[key] = {**values, "data_medicao": measured.strftime("%Y-%m-%d %H:%M:%S")}

    return list(built.values())
