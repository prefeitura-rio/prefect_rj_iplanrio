"""Helpers of rj_cor__meteorologia_redemet_estacoes."""

from collections.abc import Mapping, Sequence

from pydantic import JsonValue
from unidecode import unidecode

from pipelines.rj_cor__meteorologia_redemet_estacoes.constants import CITY, COLUMNS, KNOWN_STATIONS


def build_stations(records: Sequence[Mapping[str, JsonValue]], updated_on: str) -> list[dict[str, JsonValue]]:
    """Return the stations of Rio de Janeiro, renamed, without accents and with the update date and its
    year, month and day as text, like the legacy table.
    """
    stations: list[dict[str, JsonValue]] = []

    for record in records:
        city = record.get("cidade")

        if not isinstance(city, str) or CITY not in city:
            continue

        station = {new: record.get(old) for old, new in COLUMNS.items()}
        name = station["estacao"]
        station["estacao"] = unidecode(name) if isinstance(name, str) else name
        station["data_atualizacao"] = updated_on
        station["ano"], station["mes"], station["dia"] = updated_on.split("-")
        stations.append(station)

    return stations


def find_new_stations(stations: Sequence[Mapping[str, JsonValue]]) -> list[str]:
    """Return the ids of the stations that the REDEMET flow does not know, without repeats."""
    ids = dict.fromkeys(str(station["id_estacao"]) for station in stations)

    return [station_id for station_id in ids if station_id not in KNOWN_STATIONS]
