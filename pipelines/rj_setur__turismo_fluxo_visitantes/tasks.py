"""Tasks of rj_setur__turismo_fluxo_visitantes."""

from csv import writer
from pathlib import Path
from tempfile import mkdtemp

import gspread
from google.auth import default
from google.auth.credentials import Credentials
from googleapiclient.discovery import build
from prefect import task
from toolkit.telemetry import logger

from pipelines.rj_setur__turismo_fluxo_visitantes.helpers import (
    build_drive_query,
    build_spreadsheet_url,
    extract_year_from_filename,
    get_base_table_name,
    get_google_api_scopes,
    normalize_to_bigquery_table_name,
)
from pipelines.rj_setur__turismo_fluxo_visitantes.types import ExtractionJob


@task
def get_google_credentials() -> Credentials:
    """Get Google API credentials for Drive and Sheets access."""
    credentials, _ = default(scopes=get_google_api_scopes())
    return credentials


@task
def list_spreadsheets_in_folder(
    folder_id: str,
    credentials: Credentials,
    name_pattern: str = "Geodata_SMTUR-RJ_Escritório de dados_",
) -> list[dict[str, str]]:
    """List all spreadsheets in a Google Drive folder matching a pattern."""
    service = build("drive", "v3", credentials=credentials)
    query = build_drive_query(folder_id)
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get("files", [])

    spreadsheets = [
        {
            "name": file["name"],
            "id": file["id"],
            "url": build_spreadsheet_url(file["id"]),
            "year": extract_year_from_filename(file["name"]),
        }
        for file in files
        if name_pattern in file["name"]
    ]

    logger.info(f"Found {len(spreadsheets)} spreadsheets matching pattern '{name_pattern}'")

    for sheet in spreadsheets:
        logger.info(f"  - {sheet['name']} (Year: {sheet['year']})")

    return spreadsheets


@task
def get_worksheet_info(spreadsheet_url: str, credentials: Credentials) -> list[dict[str, str | int]]:
    """Get information about all worksheets in a spreadsheet."""
    gspread_client = gspread.authorize(credentials)
    sheet = gspread_client.open_by_url(spreadsheet_url)
    worksheets = [{"title": worksheet.title, "index": idx} for idx, worksheet in enumerate(sheet.worksheets())]

    logger.info(f"Found {len(worksheets)} worksheets in spreadsheet")

    for ws in worksheets:
        logger.info(f"  - {ws['title']} (index: {ws['index']})")

    return worksheets


@task
def extract_range_to_csv(job: ExtractionJob, credentials: Credentials) -> tuple[Path, str]:
    """Read a range of a worksheet into a CSV file, and return its folder and the table id."""
    table_id = normalize_to_bigquery_table_name(get_base_table_name(job["worksheet_title"], job["table_suffix"]))
    table_id_with_year = f"{table_id}_{job['spreadsheet_year']}"

    logger.info(f"Processing: {job['spreadsheet_name']} → {job['worksheet_title']} → {job['cell_range']}")

    sheet = gspread.authorize(credentials).open_by_url(job["spreadsheet_url"])
    rows = sheet.get_worksheet(job["worksheet_index"]).get(job["cell_range"])
    header, *body = rows
    folder = Path(mkdtemp(prefix="setur_"))

    with (folder / "data.csv").open("w", newline="", encoding="utf-8") as file:
        output = writer(file)
        output.writerow(header)
        output.writerows([*row, *[""] * (len(header) - len(row))] for row in body)

    return folder, table_id_with_year
