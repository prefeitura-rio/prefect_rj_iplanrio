import re

import pandas as pd
from janitor import clean_names


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
    """Normalize name to valid BigQuery table name using pyjanitor."""
    df = pd.DataFrame({name: []})
    df = clean_names(df)
    normalized_name = str(df.columns[0])

    if normalized_name and not normalized_name[0].isalpha() and normalized_name[0] != "_":
        normalized_name = f"table_{normalized_name}"

    return normalized_name


def get_worksheet_ranges_config() -> dict[str, list[tuple[str, str | None]]]:
    """Get configuration mapping worksheet names to their cell ranges and table suffixes."""
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
        "Municipio": [
            ("A3:D16", None),
        ],
    }


def get_google_api_scopes() -> list[str]:
    """Get required Google API scopes for Drive and Sheets access."""
    return [
        "https://www.googleapis.com/auth/drive.readonly",
        "https://www.googleapis.com/auth/spreadsheets.readonly",
    ]
