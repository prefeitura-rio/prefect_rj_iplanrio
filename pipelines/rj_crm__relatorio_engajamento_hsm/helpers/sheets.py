"""Google Sheets reads, writes and formatting for the requests and the reports.

Every write uses `RAW`, so Sheets keeps the text as it came. With `USER_ENTERED`, a
session id such as `123e4567` would become a number and a citizen line that starts with
`=` would become a formula.
"""

from collections.abc import Mapping, Sequence
from typing import TypedDict

import google.auth
import polars as pl
from googleapiclient.discovery import Resource, build
from toolkit.telemetry import logger

from pipelines.rj_crm__relatorio_engajamento_hsm.constants import (
    DRIVE_SHEET_MIME,
    FORM_EMAIL,
    FORM_TIMESTAMP,
    HEADER_TEXT_COLOR,
    SHEET_COLUMN_WIDTH,
    SHEET_LAST_ROW,
    SHEET_MAX_CELL,
    SHEET_ROW,
    SHEETS_SCOPES,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.drive import create_file
from pipelines.rj_crm__relatorio_engajamento_hsm.types import Color, DriveFile, Tab


class Highlight(TypedDict):
    """A background color for a cell whose text equals a value."""

    value: str
    color: Color


def connect() -> Resource:
    """Connect to Google Sheets with Application Default Credentials."""
    credentials, _ = google.auth.default(scopes=list(SHEETS_SCOPES))

    return build("sheets", "v4", credentials=credentials, cache_discovery=False)


def quote_tab(tab: str) -> str:
    """Return a tab name quoted for an A1 range."""
    return "'" + tab.replace("'", "''") + "'"


def column_letter(number: int) -> str:
    """Return the A1 column name of a one-based number."""
    result = ""

    while number:
        number, remainder = divmod(number - 1, 26)
        result = chr(65 + remainder) + result

    return result


def rgb(color: Color) -> dict[str, float]:
    """Return a color of the API: red, green and blue between 0 and 1."""
    return {"red": color[0], "green": color[1], "blue": color[2]}


def first_tab(sheets: Resource, sheet_id: str) -> Tab:
    """Return the id and the title of the first tab of a spreadsheet."""
    api = sheets.spreadsheets()  # pyright: ignore[reportAttributeAccessIssue]  # the Google client builds its methods at run time
    answer = api.get(spreadsheetId=sheet_id, fields="sheets.properties(sheetId,title)").execute()
    properties = answer["sheets"][0]["properties"]

    return Tab(int(properties.get("sheetId", 0)), str(properties["title"]))


def read_sheet(sheets: Resource, sheet_id: str, tab: str | None = None) -> pl.DataFrame:
    """Read one tab as text, with the first row as header.

    A short row is filled with empty text and a blank row is dropped. The column
    `sheet_row` keeps the row number of each row in the sheet.
    """
    name = tab or first_tab(sheets, sheet_id).title
    api = sheets.spreadsheets().values()  # pyright: ignore[reportAttributeAccessIssue]  # the Google client builds its methods at run time
    values = api.get(spreadsheetId=sheet_id, range=quote_tab(name)).execute().get("values", [])

    if not values:
        return pl.DataFrame()

    header = [str(value).strip() for value in values[0]]
    numbers: list[int] = []
    rows: list[list[str]] = []

    for number, raw in enumerate(values[1:], start=2):
        row = [str(value) for value in raw[: len(header)]]
        row += [""] * (len(header) - len(row))

        if any(value.strip() for value in row):
            numbers.append(number)
            rows.append(row)

    frame = pl.DataFrame(rows, schema=header, orient="row")

    return frame.insert_column(0, pl.Series(SHEET_ROW, numbers, dtype=pl.Int64))


def update_request_row(
    sheets: Resource, location: tuple[str, str], frame: pl.DataFrame, key: tuple[str, str], values: Mapping[str, str]
) -> None:
    """Write cells of the row of a request in one call, found by timestamp and email.

    `key` is the timestamp and the email.

    The column comes from the header, not from its place, so a moved column breaks
    nothing. Raise `ValueError` if the request does not match exactly one row, or if a
    column is not in the header, because a status in the wrong place is worse than none.
    """
    sheet_id, tab = location
    found = frame.filter((pl.col(FORM_TIMESTAMP) == key[0]) & (pl.col(FORM_EMAIL) == key[1]))

    if found.height != 1:
        raise ValueError(f"Expected one row of the request, found {found.height}")

    missing = [column for column in values if column not in frame.columns]

    if missing:
        raise ValueError(f"The header of {tab!r} has no column {missing}")

    row = int(found[SHEET_ROW].item())
    data = [
        {
            "range": f"{quote_tab(tab)}!{column_letter(frame.columns.index(column))}{row}",
            "values": [[text]],
        }
        for column, text in values.items()
    ]
    api = sheets.spreadsheets().values()  # pyright: ignore[reportAttributeAccessIssue]  # the Google client builds its methods at run time
    api.batchUpdate(spreadsheetId=sheet_id, body={"valueInputOption": "RAW", "data": data}).execute()


def to_cells(frame: pl.DataFrame) -> list[list[str]]:
    """Return the header and the rows as text. Nothing is empty and long text is cut."""
    cut = SHEET_MAX_CELL - 10
    rows = [
        [
            "" if value is None else (text if len(text := str(value)) <= SHEET_MAX_CELL else text[:cut] + " […]")
            for value in row
        ]
        for row in frame.iter_rows()
    ]

    return [list(frame.columns), *rows]


def rename_tab(sheets: Resource, sheet_id: str, tab: Tab, title: str) -> None:
    """Rename a tab."""
    request = {"updateSheetProperties": {"properties": {"sheetId": tab.grid_id, "title": title}, "fields": "title"}}
    api = sheets.spreadsheets()  # pyright: ignore[reportAttributeAccessIssue]  # the Google client builds its methods at run time
    api.batchUpdate(spreadsheetId=sheet_id, body={"requests": [request]}).execute()


def write_cells(sheets: Resource, sheet_id: str, title: str, cells: Sequence[Sequence[str]]) -> None:
    """Write cells from `A1` of a tab."""
    api = sheets.spreadsheets().values()  # pyright: ignore[reportAttributeAccessIssue]  # the Google client builds its methods at run time
    api.update(
        spreadsheetId=sheet_id, range=f"{quote_tab(title)}!A1", valueInputOption="RAW", body={"values": cells}
    ).execute()


def create_sheet(
    drive: Resource, sheets: Resource, folder_id: str, names: tuple[str, str], frame: pl.DataFrame
) -> DriveFile:
    """Create a spreadsheet in a Drive folder with the frame in its first tab.

    `names` is the name of the file and the name of the tab. Drive creates the file,
    because only Drive lets the caller choose the folder.
    """
    name, tab = names
    created = create_file(drive, folder_id, name, DRIVE_SHEET_MIME)
    first = first_tab(sheets, created["id"])
    rename_tab(sheets, created["id"], first, tab)
    write_cells(sheets, created["id"], tab, to_cells(frame))
    logger.info("Sheet created: name=%s rows=%d", name, frame.height)

    return created


def replace_sheet(sheets: Resource, sheet_id: str, frame: pl.DataFrame, tab: str) -> None:
    """Clear the first tab and write the frame there.

    The file keeps its id, its link and its sharing. It needs no permission to delete a
    file in a shared drive.
    """
    first = first_tab(sheets, sheet_id)
    api = sheets.spreadsheets().values()  # pyright: ignore[reportAttributeAccessIssue]  # the Google client builds its methods at run time
    api.clear(spreadsheetId=sheet_id, range=quote_tab(first.title), body={}).execute()

    if first.title != tab:
        rename_tab(sheets, sheet_id, first, tab)

    write_cells(sheets, sheet_id, tab, to_cells(frame))
    logger.info("Sheet replaced: id=%s rows=%d", sheet_id, frame.height)


def add_tab(sheets: Resource, sheet_id: str, title: str, cells: Sequence[Sequence[str]]) -> None:
    """Add a tab with cells."""
    request = {"addSheet": {"properties": {"title": title}}}
    api = sheets.spreadsheets()  # pyright: ignore[reportAttributeAccessIssue]  # the Google client builds its methods at run time
    api.batchUpdate(spreadsheetId=sheet_id, body={"requests": [request]}).execute()
    write_cells(sheets, sheet_id, title, cells)


def column_range(grid_id: int, column: int, first_row: int, last_row: int) -> dict[str, int]:
    """Return the grid range of one column between two rows, zero-based."""
    return {
        "sheetId": grid_id,
        "startRowIndex": first_row,
        "endRowIndex": last_row,
        "startColumnIndex": column,
        "endColumnIndex": column + 1,
    }


def layout_requests(grid_id: int, columns: int) -> list[dict[str, object]]:
    """Return the requests that freeze the header row and widen the columns."""
    return [
        {
            "updateSheetProperties": {
                "properties": {"sheetId": grid_id, "gridProperties": {"frozenRowCount": 1}},
                "fields": "gridProperties.frozenRowCount",
            }
        },
        {
            "updateDimensionProperties": {
                "range": {"sheetId": grid_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": columns},
                "properties": {"pixelSize": SHEET_COLUMN_WIDTH},
                "fields": "pixelSize",
            }
        },
    ]


def header_requests(grid_id: int, header: Sequence[str], colors: Mapping[str, Color]) -> list[dict[str, object]]:
    """Return the requests that color the cells of the header, found by column name.

    A column that the header does not have is skipped.
    """
    return [
        {
            "repeatCell": {
                "range": column_range(grid_id, header.index(name), 0, 1),
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": rgb(color),
                        "textFormat": {"bold": True, "foregroundColor": rgb(HEADER_TEXT_COLOR)},
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat)",
            }
        }
        for name, color in colors.items()
        if name in header
    ]


def dropdown_request(grid_id: int, column: int, options: Sequence[str]) -> dict[str, object]:
    """Return the request that limits a column to a strict list, from row 2 down."""
    return {
        "setDataValidation": {
            "range": column_range(grid_id, column, 1, SHEET_LAST_ROW),
            "rule": {
                "condition": {"type": "ONE_OF_LIST", "values": [{"userEnteredValue": option} for option in options]},
                "strict": True,
                "showCustomUi": True,
            },
        }
    }


def highlight_requests(grid_id: int, column: int, rules: Sequence[tuple[str, Color]]) -> list[dict[str, object]]:
    """Return the requests that color a column by its text, one rule for each value."""
    return [
        {
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [column_range(grid_id, column, 1, SHEET_LAST_ROW)],
                    "booleanRule": {
                        "condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": value}]},
                        "format": {"backgroundColor": rgb(color)},
                    },
                },
                "index": 0,
            }
        }
        for value, color in rules
    ]


def format_sheet(
    sheets: Resource,
    sheet_id: str,
    *,
    header_colors: Mapping[str, Color] | None = None,
    dropdowns: Mapping[str, Sequence[str]] | None = None,
    highlights: Mapping[str, Sequence[tuple[str, Color]]] | None = None,
) -> None:
    """Format the first tab in one call.

    It colors the header, freezes it, and adds dropdowns and highlights. The columns
    come from the header row. Raise `ValueError` if a dropdown or a highlight names a
    column that the header does not have. The secretariat needs a dropdown, so its
    absence must not pass in silence.
    """
    tab = first_tab(sheets, sheet_id)
    api = sheets.spreadsheets().values()  # pyright: ignore[reportAttributeAccessIssue]  # the Google client builds its methods at run time
    answer = api.get(spreadsheetId=sheet_id, range=f"{quote_tab(tab.title)}!1:1").execute()
    header = [str(cell).strip() for cell in answer.get("values", [[]])[0]]
    requests = layout_requests(tab.grid_id, len(header))
    requests += header_requests(tab.grid_id, header, header_colors or {})

    for name, options in (dropdowns or {}).items():
        requests.append(dropdown_request(tab.grid_id, position(header, name), options))

    for name, rules in (highlights or {}).items():
        requests += highlight_requests(tab.grid_id, position(header, name), rules)

    batch = sheets.spreadsheets()  # pyright: ignore[reportAttributeAccessIssue]  # the Google client builds its methods at run time
    batch.batchUpdate(spreadsheetId=sheet_id, body={"requests": requests}).execute()


def position(header: Sequence[str], name: str) -> int:
    """Return the zero-based place of a column in the header, or raise `ValueError`."""
    if name not in header:
        raise ValueError(f"The sheet has no column {name!r}")

    return header.index(name)
