"""Helpers of pipeline rj_cor__meteorologia_inmet."""

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta

from pydantic import JsonValue

from pipelines.rj_cor__meteorologia_inmet.constants import MEASURES, SP_TZ


def read_dates(first_date: str | None, last_date: str | None) -> tuple[str, str, bool]:
    """Return the first and last date to fetch, in UTC, and whether it is a backfill.

    Without a first date the range is yesterday to today, and it is not a backfill.
    """
    if first_date:
        return first_date, last_date or first_date, True

    today = datetime.now(UTC).date()

    return (today - timedelta(days=1)).isoformat(), today.isoformat(), False


def read_number(value: JsonValue) -> float | None:
    """Return a measure as a number, or nothing if it is missing or empty."""
    if value is None or value == "":
        return None

    return float(str(value))


def build_rows(
    measurements: Sequence[Mapping[str, JsonValue]], backfill: bool, today: date
) -> list[dict[str, JsonValue]]:
    """Return the measurements as rows, with the date and the time in São Paulo.

    The API sends the time in UTC as `HHMM`. `ano`, `mes` and `dia` are the day in São
    Paulo as text, which the dbt model filters on. A row with no measure is dropped.
    Without a backfill only the rows of `today` stay.
    """
    rows: list[dict[str, JsonValue]] = []

    for measurement in measurements:
        measured = datetime.strptime(
            f"{measurement['DT_MEDICAO']} {measurement['HR_MEDICAO']} +0000", "%Y-%m-%d %H%M %z"
        )
        local = measured.astimezone(SP_TZ)
        values = {name: read_number(measurement.get(code)) for code, name in MEASURES.items()}

        if all(value is None for value in values.values()):
            continue

        if not backfill and local.date() != today:
            continue

        rows.append(
            {
                "id_estacao": measurement["CD_ESTACAO"],
                "data": local.strftime("%Y-%m-%d"),
                "horario": local.strftime("%H:%M:%S"),
                "ano": f"{local:%Y}",
                "mes": f"{local:%m}",
                "dia": f"{local:%d}",
                **values,
            }
        )

    return rows
