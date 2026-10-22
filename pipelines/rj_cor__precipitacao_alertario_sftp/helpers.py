"""Helpers of rj_cor__precipitacao_alertario_sftp."""

from collections.abc import Mapping, Sequence
from datetime import datetime
from re import search
from xml.etree.ElementTree import Element

from defusedxml.ElementTree import fromstring

from pipelines.rj_cor__precipitacao_alertario_sftp.constants import (
    FILE_PATTERN,
    METEOROLOGICAL_ATTRIBUTES,
    PLUVIOMETRIC_ATTRIBUTES,
    SP_TZ,
)

Record = dict[str, str | float | None]


def parse_float(value: str | None) -> float | None:
    """Return an attribute as a number, or nothing if it is missing or is not a number."""
    try:
        return float(value) if value is not None else None
    except ValueError:
        return None


def read_file_time(name: str) -> datetime | None:
    """Return the time in a file name such as `Chuvas_20260717113729.xml`, in São Paulo, or nothing.

    ```python
    read_file_time("landing/Chuvas_20260717113729.xml")  # datetime(2026, 7, 17, 11, 37, 29, tzinfo=SP_TZ)
    ```
    """
    match = search(FILE_PATTERN, name)

    if match is None:
        return None

    try:
        return datetime.strptime(match.group(1), "%Y%m%d%H%M%S").replace(tzinfo=SP_TZ)
    except ValueError:
        return None


def read_watermark(text: str | None) -> datetime | None:
    """Return the latest time of the table as a time in São Paulo, or nothing if there is none."""
    if not text:
        return None

    try:
        return datetime.fromisoformat(text.replace("T", " ")).replace(tzinfo=SP_TZ)
    except ValueError:
        return None


def keep_new_files(names: Sequence[str], watermark: datetime | None) -> list[str]:
    """Return the XML files that the table has not seen: those after the watermark, all without one.

    Without a watermark every XML file stays. With one, a file whose name has no time is dropped.
    """
    xml_files = [name for name in names if name.endswith(".xml")]

    if watermark is None:
        return xml_files

    return [name for name in xml_files if (moment := read_file_time(name)) is not None and moment > watermark]


def parse_xml(xml: str | bytes) -> tuple[list[Record], list[Record]]:
    """Return the pluviometric and the meteorological readings of an AlertaRio XML.

    Each `<estacao>` has `<chuvas>`, and a `met` station also has `<met>`. Both are read at the
    time of `<chuvas>`, and a station without that time has no reading.
    Raise `ParseError` if the XML is malformed.
    """
    root: Element = fromstring(xml)
    pluviometric: list[Record] = []
    meteorological: list[Record] = []

    for station in root.findall("estacao"):
        rain = station.find("chuvas")
        hour = rain.get("hora") if rain is not None else None

        if rain is None or not hour:
            continue

        measured = hour.replace("T", " ")
        key: Record = {"id_estacao": station.get("id"), "data_medicao": measured}
        pluviometric.append(
            {**key, **{name: parse_float(rain.get(attribute)) for attribute, name in PLUVIOMETRIC_ATTRIBUTES.items()}}
        )
        met = station.find("met")

        if met is not None and station.get("type") == "met":
            meteorological.append(
                {
                    **key,
                    **{name: parse_float(met.get(attribute)) for attribute, name in METEOROLOGICAL_ATTRIBUTES.items()},
                }
            )

    return pluviometric, meteorological


def drop_repeats(records: Sequence[Mapping[str, str | float | None]]) -> list[Mapping[str, str | float | None]]:
    """Keep the first reading of each station and time."""
    seen: dict[tuple[str | float | None, str | float | None], Mapping[str, str | float | None]] = {}

    for record in records:
        seen.setdefault((record["id_estacao"], record["data_medicao"]), record)

    return list(seen.values())
