from csv import writer
from pathlib import Path
from tempfile import mkdtemp
from typing import TypedDict

import gspread
from google.auth import default
from google.auth.credentials import Credentials
from googleapiclient.discovery import build
from prefect import flow, task
from prefect.logging import get_logger

from pipelines.rj_setur__turismo_fluxo_visitantes.helpers import (
    build_drive_query,
    build_spreadsheet_url,
    extract_year_from_filename,
    get_base_table_name,
    get_google_api_scopes,
    get_worksheet_ranges_config,
    normalize_to_bigquery_table_name,
)
from utils.tasks import publish_table_in_bq, upload_files_to_gcs
from utils.types import Cloud, Table

logger = get_logger("toolkit")


class ExtractionJob(TypedDict):
    """Represents a single extraction job from Google Sheets to BigQuery."""

    spreadsheet_url: str
    spreadsheet_name: str
    spreadsheet_year: str
    worksheet_index: int
    worksheet_title: str
    cell_range: str
    table_suffix: str | None
    dataset_id: str
    dump_mode: str


@task
def get_google_credentials() -> Credentials:
    """Get Google API credentials for Drive and Sheets access."""
    credentials, _ = default(scopes=get_google_api_scopes())
    return credentials


@task
def list_spreadsheets_in_folder(
    folder_id: str,
    credentials,
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
def get_worksheet_info(spreadsheet_url: str, credentials) -> list[dict[str, str | int]]:
    """Get information about all worksheets in a spreadsheet."""
    gspread_client = gspread.authorize(credentials)
    sheet = gspread_client.open_by_url(spreadsheet_url)
    worksheets = [{"title": worksheet.title, "index": idx} for idx, worksheet in enumerate(sheet.worksheets())]

    logger.info(f"Found {len(worksheets)} worksheets in spreadsheet")

    for ws in worksheets:
        logger.info(f"  - {ws['title']} (index: {ws['index']})")

    return worksheets


@task
def normalize_table_name(worksheet_title: str, suffix: str | None = None) -> str:
    """Normalize worksheet title to a valid BigQuery table name using pyjanitor."""
    base_name = get_base_table_name(worksheet_title, suffix)
    normalized_name = normalize_to_bigquery_table_name(base_name)
    logger.info(f"Normalized '{base_name}' to '{normalized_name}'")
    return normalized_name


@task
def extract_range_to_csv(job: ExtractionJob, credentials: Credentials) -> tuple[Path, str]:
    """Read a range of a worksheet into a CSV file, and return its folder and the table id."""
    table_id = normalize_table_name(job["worksheet_title"], job["table_suffix"])
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


@flow(log_prints=True, flow_run_name="{dataset_id}_geodata_batch")
async def rj_setur__turismo_fluxo_visitantes(
    *,
    cloud: Cloud,
    folder_id: str = "1d_SxiMsXQd2JH1Ttik3OQDRJy7wLtX6S",
    dataset_id: str = "brutos_turismo_fluxo_visitante",
    dump_mode: str = "overwrite",
):
    """Read tourism visitor flow data from multiple Google Sheets and upload to BigQuery."""
    credentials = get_google_credentials()
    worksheet_ranges_config = get_worksheet_ranges_config()
    spreadsheets = list_spreadsheets_in_folder(folder_id=folder_id, credentials=credentials)

    if not spreadsheets:
        logger.info("No spreadsheets found in folder")
        return

    extraction_jobs: list[ExtractionJob] = []

    for spreadsheet in spreadsheets:
        spreadsheet_url = spreadsheet["url"]
        spreadsheet_name = spreadsheet["name"]
        spreadsheet_year = spreadsheet["year"]
        worksheets = get_worksheet_info(spreadsheet_url=spreadsheet_url, credentials=credentials)

        for worksheet in worksheets:
            worksheet_title = str(worksheet["title"])
            worksheet_index = int(worksheet["index"])

            if worksheet_title not in worksheet_ranges_config:
                continue

            ranges_config = worksheet_ranges_config[worksheet_title]

            for cell_range, table_suffix in ranges_config:
                job: ExtractionJob = {
                    "spreadsheet_url": spreadsheet_url,
                    "spreadsheet_name": spreadsheet_name,
                    "spreadsheet_year": spreadsheet_year,
                    "worksheet_index": worksheet_index,
                    "worksheet_title": worksheet_title,
                    "cell_range": cell_range,
                    "table_suffix": table_suffix,
                    "dataset_id": dataset_id,
                    "dump_mode": dump_mode,
                }
                table_name = f"{worksheet_title}{'_' + table_suffix if table_suffix else ''}_{spreadsheet_year}"
                logger.info(f"  Queued: {table_name} from {cell_range}")
                extraction_jobs.append(job)

    logger.info(f"\nPrepared {len(extraction_jobs)} extraction tasks")
    logger.info("Starting sequential extraction...\n")

    for job in extraction_jobs:
        folder, table_id = extract_range_to_csv(job, credentials)
        table = Table(dataset_id=job["dataset_id"], table_id=table_id)
        await upload_files_to_gcs(
            folder, table, bucket=cloud["bucket"], dump_mode=job["dump_mode"], source_format="csv"
        )
        await publish_table_in_bq(table, cloud)

    logger.info(f"\n✓ All {len(extraction_jobs)} extractions completed successfully")
