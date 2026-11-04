"""The Google Sheet of the aggregated clusters."""

from datetime import datetime, timedelta

import gspread
from gspread.utils import ValueInputOption

from pipelines.rj_iplanrio__cor_alerts_aggregator.constants import (
    DEFAULT_RETENTION_HOURS,
    SHEET_COLUMNS,
    SHEET_SCOPES,
    SHEET_TAB,
    SP_TZ,
)
from pipelines.rj_iplanrio__cor_alerts_aggregator.helpers.rows import plan_sheet_update
from pipelines.rj_iplanrio__cor_alerts_aggregator.types import Row, Sheet
from utils.utils import load_credentials


def update_sheet(rows: list[Row], sheet: Sheet) -> int:
    """Bring the sheet up to date with the rows, and return how many rows it wrote."""
    client = gspread.authorize(load_credentials(SHEET_SCOPES))
    worksheet = client.open_by_key(sheet["spreadsheet_id"]).worksheet(SHEET_TAB)
    cutoff = datetime.now(SP_TZ) - timedelta(hours=sheet.get("retention_hours", DEFAULT_RETENTION_HOURS))
    plan = plan_sheet_update(worksheet.get_all_values(), rows, cutoff)

    if plan.clear:
        worksheet.clear()

    if plan.header:
        worksheet.append_row(list(SHEET_COLUMNS.values()))

    for number in sorted(plan.stale_rows, reverse=True):
        worksheet.delete_rows(number)

    if plan.updates:
        worksheet.batch_update(
            [{"range": f"A{number}", "values": [line]} for number, line in plan.updates.items()],
            value_input_option=ValueInputOption.raw,
        )

    if plan.appends:
        worksheet.append_rows(plan.appends, value_input_option=ValueInputOption.raw)

    return len(plan.updates) + len(plan.appends)
