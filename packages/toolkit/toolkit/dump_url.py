"""Download a URL, Google Drive file or Google Sheet and stage it."""

from enum import StrEnum
from pathlib import Path
from typing import cast

import google.auth
import gspread
import polars as pl
import requests
from google.auth.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from psycopg.sql import SQL, Composable, Literal

from .log import logger
from .types import NonEmptyString

SHEETS_PREFIX = "https://docs.google.com/spreadsheets/d/"
DRIVE_PREFIX = "https://drive.google.com/file/d/"
SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive"]
DOWNLOAD_TIMEOUT_SECONDS = 300
CHUNK_SIZE = 8192


class UrlType(StrEnum):
    """Supported URL kinds."""

    DIRECT = "direct"
    GOOGLE_DRIVE = "google_drive"
    GOOGLE_SHEET = "google_sheet"


def drive_file_id(url: str) -> str:
    """Return the file id of a Google Drive file URL.

    :raises ValueError: If the URL is not a Drive file URL.
    """
    if not url.startswith(DRIVE_PREFIX):
        raise ValueError(f"Google Drive URL must start with {DRIVE_PREFIX}: {url}")
    return url.removeprefix(DRIVE_PREFIX).split("/")[0]


def download_direct(url: str, path: Path) -> None:
    """Stream a direct URL to a file."""
    with requests.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT_SECONDS) as response:
        response.raise_for_status()
        with path.open("wb") as file:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                file.write(chunk)


def workload_credentials(scopes: list[str]) -> Credentials:
    """Return the workload identity credentials for the scopes."""
    credentials, _ = google.auth.default(scopes=scopes)
    return cast("Credentials", credentials)


def download_drive(url: str, path: Path) -> None:
    """Download a Drive file with the workload identity credentials."""
    credentials = workload_credentials(DRIVE_SCOPES)
    service = build("drive", "v3", credentials=credentials)
    request = service.files().get_media(fileId=drive_file_id(url))
    with path.open("wb") as file:
        downloader = MediaIoBaseDownload(file, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()


def download_sheet(
    url: str,
    path: Path,
    sheet_order: int = 0,
    sheet_name: str | None = None,
    sheet_range: str | None = None,
) -> None:
    """Write one worksheet to CSV. The first row is the header.

    :raises ValueError: If the URL is not a Sheets URL or the worksheet is empty.
    """
    if not url.startswith(SHEETS_PREFIX):
        raise ValueError(f"Google Sheets URL must start with {SHEETS_PREFIX}: {url}")
    credentials = workload_credentials(SHEETS_SCOPES)
    sheet = gspread.authorize(credentials).open_by_url(url)
    worksheet = (
        sheet.worksheet(sheet_name) if sheet_name else sheet.get_worksheet(sheet_order)
    )
    values = cast(
        "list[list[str]]",
        worksheet.batch_get((sheet_range,))[0]
        if sheet_range
        else worksheet.get_values(),
    )
    if len(values) < 2:
        raise ValueError("No data found in the selected worksheet")
    pl.DataFrame(values[1:], schema=values[0], orient="row").write_csv(path)


def download(
    url: str,
    path: Path,
    url_type: UrlType = UrlType.DIRECT,
    sheet_order: int = 0,
    sheet_name: str | None = None,
    sheet_range: str | None = None,
) -> Path:
    """Download one URL to a local file.

    :param url: Source URL.
    :param path: Destination file. Its folder is created.
    :param url_type: Kind of URL.
    :returns: ``path``.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    match url_type:
        case UrlType.DIRECT:
            download_direct(url, path)
        case UrlType.GOOGLE_DRIVE:
            download_drive(url, path)
        case UrlType.GOOGLE_SHEET:
            download_sheet(url, path, sheet_order, sheet_name, sheet_range)
    logger.info("File downloaded: type=%s", url_type)
    return path


def csv_scan(
    path: Path,
    separator: NonEmptyString = ",",
    encoding: NonEmptyString = "utf-8",
    *,
    ignore_errors: bool = False,
) -> Composable:
    """Return the DuckDB ``FROM`` expression that reads a CSV file.

    :param path: CSV file.
    :param separator: Column delimiter.
    :param encoding: File encoding (``utf-8``, ``utf-16`` or ``latin-1``).
    :param ignore_errors: Skip rows that do not parse, like ``on_bad_lines='skip'``.
    """
    return SQL("read_csv({}, delim = {}, encoding = {}, ignore_errors = {})").format(
        Literal(str(path)),
        Literal(separator),
        Literal(encoding),
        SQL("true" if ignore_errors else "false"),
    )
