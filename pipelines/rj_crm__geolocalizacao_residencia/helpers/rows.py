"""The rows of the result: the address, the geocode and the plus code, all as text."""

from collections.abc import Mapping, Sequence

from openlocationcode import openlocationcode as olc
from toolkit.telemetry import logger

from pipelines.rj_crm__geolocalizacao_residencia.constants import GEOCODE_FIELDS, MAX_LATITUDE, MAX_LONGITUDE
from pipelines.rj_crm__geolocalizacao_residencia.types import Address, Geocode, Row


def texts(addresses: Sequence[Address]) -> list[str]:
    """Return the full address of each address."""
    return [address.get("endereco_completo") or "" for address in addresses]


def split_found(
    addresses: Sequence[Address], geocodes: Sequence[Geocode]
) -> tuple[list[Address], list[Geocode], list[Address]]:
    """Split the addresses into those with a point, their geocodes and those left."""
    found: list[Address] = []
    points: list[Geocode] = []
    left: list[Address] = []

    for address, geocode in zip(addresses, geocodes, strict=True):
        if has_point(geocode):
            found.append(address)
            points.append(geocode)
        else:
            left.append(address)

    return found, points, left


def to_pluscode(latitude: str, longitude: str) -> str | None:
    """Return the Google Plus Code of a point, or nothing if the coordinates are bad.

    Coordinates that are not numbers, or out of range, are bad.

    ```python
    to_pluscode("-22.907104", "-43.172897")  # "589R3RVG+5R"
    ```
    """
    try:
        lat, lng = float(latitude), float(longitude)
    except ValueError:
        return None

    if abs(lat) > MAX_LATITUDE or abs(lng) > MAX_LONGITUDE:
        logger.info("A point is out of range: latitude=%s longitude=%s", latitude, longitude)
        return None

    return olc.encode(lat, lng)


def stringify(value: str | float | None) -> str:
    """Return a value as text, with nothing as empty text."""
    return "" if value is None else str(value)


def has_point(geocode: Mapping[str, str | float | None]) -> bool:
    """Tell whether a geocode has a latitude and a longitude."""
    return geocode.get("latitude") is not None and geocode.get("longitude") is not None


def join_rows(addresses: Sequence[Address], geocodes: Sequence[Geocode], *, need_street: bool) -> list[Row]:
    """Return a text row for each address that got a point.

    The row keeps every column of the address, and the geocode and the plus code
    follow them.

    With `need_street`, as in the fallback, an address whose geocode has no street is
    dropped, and so is an address whose treated street is the text `nan`.
    """
    rows: list[Row] = []

    for address, geocode in zip(addresses, geocodes, strict=True):
        if not has_point(geocode):
            continue

        if need_street and (geocode.get("logradouro_geocode") is None or address.get("logradouro_tratado") == "nan"):
            continue

        row: Row = {column: stringify(value) for column, value in address.items()}
        row.update({field: stringify(geocode.get(field)) for field in GEOCODE_FIELDS})
        row["pluscode"] = to_pluscode(row["latitude"], row["longitude"]) or ""
        rows.append(row)

    return rows
