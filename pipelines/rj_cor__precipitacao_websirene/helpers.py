"""Helpers of rj_cor__precipitacao_websirene."""

from datetime import UTC, datetime
from xml.etree.ElementTree import Element, fromstring

from pipelines.rj_cor__precipitacao_websirene.constants import RAIN_COLUMNS, SP_TZ


def read_number(text: str | None) -> float | None:
    """Return an attribute as a number, or nothing if it is missing or not a number."""
    try:
        return float(text) if text is not None else None
    except ValueError:
        return None


def read_time(text: str) -> str:
    """Return a time of the XML in São Paulo as `YYYY-MM-DD HH:MM:SS`.

    A time without a zone is UTC.
    """
    parsed = datetime.fromisoformat(text)
    parsed = parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)

    return parsed.astimezone(SP_TZ).strftime("%Y-%m-%d %H:%M:%S")


def build_rows(xml: str | bytes) -> list[dict[str, str | float | None]]:
    """Return one row for each XML station with a rain reading, in São Paulo time."""
    root: Element = fromstring(xml)  # the page is the public one of the city
    rows: list[dict[str, str | float | None]] = []

    for station in root.iter("estacao"):
        rain = station.find("chuvas")

        if rain is None or "hora" not in rain.attrib:
            continue

        rows.append(
            {
                "id_estacao": station.get("id"),
                "data_medicao": read_time(rain.attrib["hora"]),
                **{name: read_number(rain.get(attribute)) for attribute, name in RAIN_COLUMNS.items()},
            }
        )

    return rows
