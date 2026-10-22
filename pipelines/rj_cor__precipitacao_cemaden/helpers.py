"""Helpers of rj_cor__precipitacao_cemaden."""

from collections.abc import Mapping, Sequence
from datetime import datetime

from pydantic import JsonValue

from pipelines.rj_cor__precipitacao_cemaden.constants import (
    CITY_CODE,
    KNOWN_STATIONS,
    PLUVIOMETRIC,
    RAIN_COLUMNS,
    SP_TZ,
)


def read_rain(value: JsonValue) -> float:
    """Return a rain reading as a number, with nothing, `-` and a negative number as 0."""
    try:
        return max(float(str(value)), 0.0)
    except ValueError:
        return 0.0


def read_coordinate(value: JsonValue) -> float | None:
    """Return a latitude or a longitude as a number, or nothing if it is not one."""
    try:
        return float(str(value))
    except ValueError:
        return None


def build_rows(records: Sequence[Mapping[str, JsonValue]]) -> list[dict[str, JsonValue]]:
    """Return the readings of the rain stations of Rio, with the time in São Paulo.

    The API sends the time in UTC. A repeated station and time keeps the first.
    """
    rows: dict[tuple[JsonValue, str], dict[str, JsonValue]] = {}

    for record in records:
        if record.get("codibge") != CITY_CODE or record.get("tipoestacao") != PLUVIOMETRIC:
            continue

        measured = (
            datetime.strptime(f"{record['datahora']} +0000", "%Y-%m-%d %H:%M:%S %z")
            .astimezone(SP_TZ)
            .strftime("%Y-%m-%d %H:%M:%S")
        )
        key = (record["idestacao"], measured)

        if key in rows:
            continue

        rows[key] = {
            "id_estacao": record["idestacao"],
            "latitude": read_coordinate(record.get("latitude")),
            "longitude": read_coordinate(record.get("longitude")),
            **{name: read_rain(record.get(source)) for source, name in RAIN_COLUMNS.items()},
            "data_medicao": measured,
        }

    return list(rows.values())


def find_new_stations(rows: Sequence[Mapping[str, JsonValue]]) -> list[int]:
    """Return the ids of the stations that the flow does not know, without repeats."""
    ids = dict.fromkeys(int(str(row["id_estacao"])) for row in rows)

    return [station for station in ids if station not in KNOWN_STATIONS]
