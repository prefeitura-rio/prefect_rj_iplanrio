from typing import TypedDict

import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from iplanrio.pipelines_templates.dump_url.flows import dump_url_flow
from iplanrio.pipelines_utils.env import (
    get_credentials_from_env,
    inject_bd_credentials_task,
)
from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow, task

from .utils import (
    build_drive_query,
    build_spreadsheet_url,
    extract_year_from_filename,
    get_base_table_name,
    get_google_api_scopes,
    get_worksheet_ranges_config,
    normalize_to_bigquery_table_name,
)


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
    biglake_table: bool


@task
def get_google_credentials() -> Credentials:
    """Get Google API credentials for Drive and Sheets access."""
    return get_credentials_from_env(scopes=get_google_api_scopes())


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

    log(f"Found {len(spreadsheets)} spreadsheets matching pattern '{name_pattern}'")

    for sheet in spreadsheets:
        log(f"  - {sheet['name']} (Year: {sheet['year']})")

    return spreadsheets


@task
def get_worksheet_info(spreadsheet_url: str, credentials) -> list[dict[str, str | int]]:
    """Get information about all worksheets in a spreadsheet."""
    gspread_client = gspread.authorize(credentials)
    sheet = gspread_client.open_by_url(spreadsheet_url)
    worksheets = [{"title": worksheet.title, "index": idx} for idx, worksheet in enumerate(sheet.worksheets())]

    log(f"Found {len(worksheets)} worksheets in spreadsheet")

    for ws in worksheets:
        log(f"  - {ws['title']} (index: {ws['index']})")

    return worksheets


@task
def normalize_table_name(worksheet_title: str, suffix: str | None = None) -> str:
    """Normalize worksheet title to a valid BigQuery table name using pyjanitor."""
    base_name = get_base_table_name(worksheet_title, suffix)
    normalized_name = normalize_to_bigquery_table_name(base_name)
    log(f"Normalized '{base_name}' to '{normalized_name}'")
    return normalized_name


@task
def extract_range_to_bigquery(job: ExtractionJob) -> None:
    """Extract a specific range from a spreadsheet worksheet and upload to BigQuery."""
    table_id = normalize_table_name(job["worksheet_title"], job["table_suffix"])
    table_id_with_year = f"{table_id}_{job['spreadsheet_year']}"

    log(f"Processing: {job['spreadsheet_name']} → {job['worksheet_title']} → {job['cell_range']}")
    log(f"  Uploading to {job['dataset_id']}.{table_id_with_year}")

    dump_url_flow(
        url=job["spreadsheet_url"],
        url_type="google_sheet",
        gsheets_sheet_order=job["worksheet_index"],
        gsheets_sheet_name=job["worksheet_title"],
        gsheets_sheet_range=job["cell_range"],
        dataset_id=job["dataset_id"],
        table_id=table_id_with_year,
        dump_mode=job["dump_mode"],
        partition_columns="",
        biglake_table=job["biglake_table"],
    )

    log(f"  ✓ Completed {job['dataset_id']}.{table_id_with_year}")


@flow(log_prints=True)
def rj_setur__turismo_fluxo_visitantes(
    folder_id: str = "1d_SxiMsXQd2JH1Ttik3OQDRJy7wLtX6S",
    dataset_id: str = "brutos_turismo_fluxo_visitante",
    dump_mode: str = "overwrite",
):
    """Read tourism visitor flow data from multiple Google Sheets and upload to BigQuery."""
    rename_current_flow_run_task(new_name=f"{dataset_id}_geodata_batch")
    inject_bd_credentials_task(environment="prod")

    credentials = get_google_credentials()
    worksheet_ranges_config = get_worksheet_ranges_config()
    spreadsheets = list_spreadsheets_in_folder(folder_id=folder_id, credentials=credentials)

    if not spreadsheets:
        log("No spreadsheets found in folder")
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
                    "biglake_table": False,
                }
                table_name = f"{worksheet_title}{'_' + table_suffix if table_suffix else ''}_{spreadsheet_year}"
                log(f"  Queued: {table_name} from {cell_range}")
                extraction_jobs.append(job)

    log(f"\nPrepared {len(extraction_jobs)} extraction tasks")
    log("Starting sequential extraction...\n")

    for job in extraction_jobs:
        extract_range_to_bigquery(job)

    log(f"\n✓ All {len(extraction_jobs)} extractions completed successfully")
