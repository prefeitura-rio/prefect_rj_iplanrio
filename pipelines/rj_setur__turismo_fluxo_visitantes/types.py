"""Types of rj_setur__turismo_fluxo_visitantes."""

from typing import TypedDict

from utils.types import DumpMode


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
    dump_mode: DumpMode
