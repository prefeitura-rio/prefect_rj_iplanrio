"""Helpers of rj_setur__turismo_fluxo_visitantes."""

import re
from collections.abc import Mapping, Sequence

import gspread
from google.auth.credentials import Credentials
from toolkit.telemetry import logger
from unidecode import unidecode

from pipelines.rj_setur__turismo_fluxo_visitantes.types import ExtractionJob
from utils.types import DumpMode


def extract_year_from_filename(filename: str) -> str:
    """Extract year from filename ending with _YYYY pattern."""
    year_match = re.search(r"_(\d{4})$", filename)
    return year_match.group(1) if year_match else "unknown"


def build_spreadsheet_url(spreadsheet_id: str) -> str:
    """Build Google Sheets URL from spreadsheet ID."""
    return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/"


def build_drive_query(folder_id: str) -> str:
    """Build Google Drive API query to find spreadsheets in folder."""
    mime_type = "application/vnd.google-apps.spreadsheet"
    return f"'{folder_id}' in parents and mimeType='{mime_type}' and trashed=false"


def get_base_table_name(worksheet_title: str, suffix: str | None) -> str:
    """Get base table name by combining worksheet title and optional suffix."""
    if suffix:
        return f"{worksheet_title}_{suffix}"
    return worksheet_title


def normalize_to_bigquery_table_name(name: str) -> str:
    """Normalize name to valid BigQuery table name."""
    normalized_name = unidecode(name.lower())
    normalized_name = re.sub(r"[^a-z0-9_]", "_", normalized_name)
    normalized_name = re.sub(r"_+", "_", normalized_name)
    return normalized_name.strip("_")


def get_worksheet_ranges_config() -> dict[str, list[tuple[str, str | None]]]:
    """Return the config that maps worksheet names to cell ranges and table suffixes."""
    return {
        "Dados atrativos": [
            ("A4:N17", "visitantes"),
            ("A23:N36", "turistas"),
        ],
        "Ranking estrangeiros": [
            ("A3:P25", None),
        ],
        "Ranking nacional por estado": [
            ("A3:O30", None),
        ],
        "Município": [
            ("A3:D16", None),
        ],
    }


def get_google_api_scopes() -> list[str]:
    """Get required Google API scopes for Drive and Sheets access."""
    return [
        "https://www.googleapis.com/auth/drive.readonly",
        "https://www.googleapis.com/auth/spreadsheets.readonly",
    ]


def read_worksheet_titles(spreadsheet_url: str, credentials: Credentials) -> list[str]:
    """Read the titles of the worksheets of a spreadsheet, in the order of its tabs."""
    sheet = gspread.authorize(credentials).open_by_url(spreadsheet_url)
    titles = [worksheet.title for worksheet in sheet.worksheets()]

    logger.info(f"Found {len(titles)} worksheets in spreadsheet: {titles}")

    return titles


def build_extraction_jobs(
    spreadsheet: Mapping[str, str],
    titles: Sequence[str],
    ranges: Mapping[str, Sequence[tuple[str, str | None]]],
    dataset_id: str,
    dump_mode: DumpMode,
) -> list[ExtractionJob]:
    """Build one job for each range that the config has for the worksheets.

    They are the worksheets of one spreadsheet.

    A worksheet that the config does not list gives no job. The index of a worksheet is
    the position of its title.
    """
    return [
        ExtractionJob(
            spreadsheet_url=spreadsheet["url"],
            spreadsheet_name=spreadsheet["name"],
            spreadsheet_year=spreadsheet["year"],
            worksheet_index=index,
            worksheet_title=title,
            cell_range=cell_range,
            table_suffix=table_suffix,
            dataset_id=dataset_id,
            dump_mode=dump_mode,
        )
        for index, title in enumerate(titles)
        for cell_range, table_suffix in ranges.get(title, [])
    ]
