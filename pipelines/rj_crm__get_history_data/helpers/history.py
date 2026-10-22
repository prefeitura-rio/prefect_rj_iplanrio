"""The rows of the history: one for each record of each Data Extension, and the window of days that a run takes."""

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from json import dumps
from re import sub

from pydantic import JsonValue
from toolkit.telemetry import logger

from pipelines.rj_crm__get_history_data.constants import DATE_FORMATS, DE_SUFFIX, MERGE_KEYS
from pipelines.rj_crm__get_history_data.types import Extension, Extraction, Field, Row


def normalize_field_name(name: str) -> str:
    """Return a field name to compare: lower case, with each run of spaces, `_` and `-` made one space."""
    return sub(r"[\s_-]+", " ", name.strip().lower())


def keep_history(extensions: Sequence[Extension]) -> list[Extension]:
    """Return the Data Extensions whose name ends in `historico`, in any case."""
    return [extension for extension in extensions if extension.name.lower().endswith(DE_SUFFIX)]


def find_columns(fields: Sequence[Field]) -> tuple[str | None, str | None, str | None]:
    """Return the names of the phone column, the primary key column and the entry date column of a Data Extension.

    The entry date column is the one whose name has `data de entrada`. Failing that, it is the only column of type
    `date`, if there is just one.
    """
    phone = next((item.name for item in fields if item.type.lower() == "phone"), None)
    primary = next((item.name for item in fields if item.is_pk), None)
    entry = next((item.name for item in fields if "data de entrada" in normalize_field_name(item.name)), None)

    if entry is None:
        dates = [item.name for item in fields if item.type.lower() == "date"]
        entry = dates[0] if len(dates) == 1 else None

    return phone, primary, entry


def merge_record(record: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
    """Return a record of the REST API as one object.

    The API gives `{"keys": {...}, "values": {...}}`, and only the primary key is in `keys`, so both are joined.
    """
    if "keys" in record or "values" in record:
        keys, values = record.get("keys"), record.get("values")

        return {**(keys if isinstance(keys, dict) else {}), **(values if isinstance(values, dict) else {})}

    return dict(record)


def build_rows(results: Sequence[Extraction]) -> list[Row]:
    """Return a row for each record of the Data Extensions that worked.

    A row has the name of the extension, the phone, the primary key, the entry date and the whole record as JSON. The
    keys of the records come in lower case from the REST API and the schema has the official capitals, so the lookup
    ignores case.
    """
    rows: list[Row] = []

    for result in results:
        if result.error is not None:
            continue

        phone, primary, entry = find_columns(result.fields)

        for record in result.records:
            merged = merge_record(record)
            lowered = {key.lower(): value for key, value in merged.items()}
            rows.append(
                {
                    "de_nome": result.name,
                    "telefone": lowered.get(phone.lower()) if phone else None,
                    "id_de": lowered.get(primary.lower()) if primary else None,
                    "entrada_data": lowered.get(entry.lower()) if entry else None,
                    "dados_json": dumps(merged, ensure_ascii=False),
                }
            )

    return rows


def parse_entry_date(value: JsonValue) -> datetime | None:
    """Return an entry date as a time in UTC without a zone, or nothing if it is not a date.

    The ISO format comes first. Then the formats of SFMC, month first before day first, so `9/15/2026 3:04:05 PM` and
    `15/09/2026` both read. A time with no zone counts as UTC.
    """
    if not isinstance(value, str) or not value.strip():
        return None

    text = value.strip()
    moment: datetime | None = None

    try:
        moment = datetime.fromisoformat(text)
    except ValueError:
        for date_format in DATE_FORMATS:
            try:
                moment = datetime.strptime(text, date_format).replace(tzinfo=UTC)
                break
            except ValueError:
                continue

    if moment is None:
        return None

    return (moment if moment.tzinfo else moment.replace(tzinfo=UTC)).astimezone(UTC).replace(tzinfo=None)


def filter_recent(rows: Sequence[Row], window_days: int, now: datetime) -> list[Row]:
    """Return the rows of the last `window_days` days, with `data_particao`, and without repeats of the merge key.

    A row with no entry date that can be read is always kept, since its age is unknown, and takes the day of the run
    as `data_particao`. The first row of each key stays.
    """
    cutoff = now.astimezone(UTC).replace(tzinfo=None) - timedelta(days=window_days)
    kept: dict[tuple[str, ...], Row] = {}

    for row in rows:
        entered = parse_entry_date(row["entrada_data"])

        if entered is not None and entered < cutoff:
            continue

        stamped: Row = {
            **row,
            "data_particao": (entered or now.astimezone(UTC).replace(tzinfo=None)).date().isoformat(),
        }
        kept.setdefault(tuple(repr(row[key]) for key in MERGE_KEYS), stamped)

    logger.info("Window of %d days: %d rows in, %d rows out", window_days, len(rows), len(kept))

    return list(kept.values())
