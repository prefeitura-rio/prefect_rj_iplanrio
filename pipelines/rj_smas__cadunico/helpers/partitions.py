"""Read the date and the layout version of a raw file of records."""

from datetime import UTC, datetime
from pathlib import Path

from pipelines.rj_smas__cadunico.constants import (
    DATE_IN_HEADER,
    FULL_DATE_LENGTH,
    HEADER_DATE_FORMAT,
    MONTH_DATE_LENGTH,
    TWO_DIGIT_YEAR_LENGTH,
    VERSION_IN_HEADER,
)


def reformat_date(text: str, pattern: str) -> str:
    """Return a date as `YYYY-MM-DD`, from its text and the `strptime` pattern."""
    return datetime.strptime(text, pattern).replace(tzinfo=UTC).date().isoformat()


def parse_partition(blob_name: str) -> str:
    """Return the date `YYYY-MM-DD` that the name of a raw file holds.

    The date is `A` and `YYMMDD`, or `YYYYMMDD`, or `YYYYMM` for the first day of a month.
    Raise `ValueError` if the name has no date.
    """
    stem = blob_name.rsplit("_", maxsplit=1)[-1] if "_" in blob_name else blob_name

    for part in stem.split("."):
        if part.startswith("A") and len(part) == TWO_DIGIT_YEAR_LENGTH:
            return reformat_date(part[1:], "%y%m%d")

        if part.isdigit() and len(part) == FULL_DATE_LENGTH:
            return reformat_date(part, "%Y%m%d")

        if part.isdigit() and len(part) == MONTH_DATE_LENGTH:
            return reformat_date(f"{part}01", "%Y%m%d")

    raise ValueError(f"No partition info found in blob name: {blob_name}")


def read_header(path: Path) -> tuple[str, str]:
    """Return the layout version and the date `YYYY-MM-DD` of the extraction, from the first line of a file."""
    with path.open(encoding="utf-8", errors="replace") as lines:
        first = lines.readline()

    version = first[VERSION_IN_HEADER].strip().replace(".", "")

    return version, reformat_date(first[DATE_IN_HEADER].strip(), HEADER_DATE_FORMAT)
