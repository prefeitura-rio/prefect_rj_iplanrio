"""The geocoding services: the request for an address, and how to read the answer.

Each service answers in its own shape, and the parsers bring them to the same fields. A
parser gives nothing when the answer has no match, or the service refuses the request
with the status of an exhausted quota.
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Final

from httpx2 import Response
from pydantic import JsonValue
from unidecode import unidecode

from pipelines.rj_crm__geolocalizacao_residencia.constants import RIO_CONTEXT, WAZE_BOUNDS
from pipelines.rj_crm__geolocalizacao_residencia.types import Geocode
from utils.records import dig, read_json

OK: Final = 200
PAYMENT_REQUIRED: Final = 402
TOO_MANY_REQUESTS: Final = 429
CONFIDENCE_SCALE: Final = 10
LOCATIONIQ_CITY_PART: Final = 2
LOCATIONIQ_STATE_PART: Final = 4


@dataclass(frozen=True)
class Query:
    """A GET request to a service."""

    url: str
    params: dict[str, str] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class Provider:
    """A geocoding service: its `geocode` name, its request and its reader.

    `parse` takes the address, the response and the day, and gives the geocode or
    nothing.
    """

    name: str
    build: Callable[[str], Query]
    parse: Callable[[Response, str], Geocode | None]


def fold(value: JsonValue, *, strip: bool = True) -> str | None:
    """Return a text in lower case without accents, or nothing if it is not text."""
    if not isinstance(value, str) or not value:
        return None

    text = unidecode(value).lower()

    return text.strip() if strip else text


def text_of(value: JsonValue) -> str | None:
    """Return a value as text, or nothing if it is empty."""
    return str(value).strip() or None if value is not None else None


def as_float(value: JsonValue) -> float | None:
    """Return a value as a number, or nothing if it is not one."""
    try:
        return float(value) if isinstance(value, str | int | float) else None
    except ValueError:
        return None


def empty_geocode(provider: str, today: str) -> Geocode:
    """Return the geocode of an address that the service could not find: all empty."""
    return {
        "latitude": None,
        "longitude": None,
        "logradouro_geocode": None,
        "numero_porta_geocode": None,
        "bairro_geocode": None,
        "cidade_geocode": None,
        "estado_geocode": None,
        "cep_geocode": None,
        "confianca": None,
        "updated_date": today,
        "geocode": provider,
    }


def add_rio(address: str) -> str:
    """Return the address with `Rio de Janeiro` at the end, unless it has it."""
    return address if RIO_CONTEXT in address else f"{address}, {RIO_CONTEXT}"


def nominatim(domain: str, user_agent: str) -> Provider:
    """Return the Nominatim server of a domain.

    The address gets `Rio de Janeiro`, and `Brasil` goes to English.
    """

    def build(address: str) -> Query:
        search = add_rio(address).replace(" RJ,", "").replace("Brasil", "Brazil")
        params = {"q": search, "format": "json", "addressdetails": "1", "countrycodes": "br", "limit": "1"}

        return Query(f"https://{domain}/search", params, {"User-Agent": user_agent})

    def parse(response: Response, today: str) -> Geocode | None:
        data = read_json(response) if response.status_code == OK else None
        first = data[0] if isinstance(data, list) and data else None

        if not isinstance(first, dict):
            return None

        return {
            "latitude": text_of(first.get("lat")),
            "longitude": text_of(first.get("lon")),
            "logradouro_geocode": fold(dig(first, "address", "road"), strip=False),
            "numero_porta_geocode": text_of(dig(first, "address", "house_number")),
            "bairro_geocode": fold(dig(first, "address", "suburb"), strip=False),
            "cidade_geocode": fold(dig(first, "address", "city"), strip=False),
            "estado_geocode": fold(dig(first, "address", "state"), strip=False),
            "cep_geocode": text_of(dig(first, "address", "postcode")),
            "confianca": None,
            "updated_date": today,
            "geocode": "nominatim",
        }

    return Provider("nominatim", build, parse)


def waze_query(address: str) -> Query:
    """Return the request to the autocomplete of Waze, bounded to the city of Rio."""
    params = {"q": address, "exp": "8,10,12", "geo-env": "row", "v": WAZE_BOUNDS, "lang": "pt-BR"}

    return Query("https://www.waze.com/live-map/api/autocomplete/", params)


def parse_waze(response: Response, today: str) -> Geocode | None:
    """Read the first place of a Waze answer.

    Its name is `street, neighborhood, city`.
    """
    data = read_json(response) if response.status_code == OK else None
    first = data[0] if isinstance(data, list) and data else None

    if not isinstance(first, dict):
        return None

    full_name = dig(first, "fullName") or dig(first, "name") or ""
    parts = str(full_name).split(", ")
    street, neighborhood = parts[0] if parts else "", parts[1] if len(parts) > 1 else ""
    city = parts[2] if len(parts) > 2 else RIO_CONTEXT  # noqa: PLR2004

    return {
        "latitude": as_float(dig(first, "location", "lat")),
        "longitude": as_float(dig(first, "location", "lon")),
        "logradouro_geocode": fold(street),
        "numero_porta_geocode": None,
        "bairro_geocode": fold(neighborhood),
        "cidade_geocode": fold(city) or "rio de janeiro",
        "estado_geocode": "rio de janeiro",
        "cep_geocode": None,
        "confianca": 1.0,
        "updated_date": today,
        "geocode": "waze",
    }


WAZE: Final = Provider("waze", waze_query, parse_waze)


def maptiler(key: str) -> Provider:
    """Return MapTiler with a key.

    The address goes in the path, with `Rio de Janeiro` and `Brazil` added.
    """

    def build(address: str) -> Query:
        search = f"{address}, {RIO_CONTEXT}, Brazil" if RIO_CONTEXT not in address else f"{address}, Brazil"

        return Query(
            f"https://api.maptiler.com/geocoding/{search}.json",
            {"key": key, "language": "pt", "limit": "1", "country": "br"},
        )

    def parse(response: Response, today: str) -> Geocode | None:
        if response.status_code in (PAYMENT_REQUIRED, TOO_MANY_REQUESTS):
            return None

        features = dig(read_json(response), "features")
        first = features[0] if isinstance(features, list) and features else None

        if not isinstance(first, dict):
            return None

        coordinates = dig(first, "geometry", "coordinates")
        found = coordinates if isinstance(coordinates, list) else []
        context = dig(first, "properties", "context")
        names: dict[str, str] = {}

        for item in context if isinstance(context, list) else []:
            kind = str(dig(item, "id") or "").split(".")[0]
            names[{"locality": "neighborhood"}.get(kind, kind)] = str(dig(item, "text") or "")

        return {
            "latitude": as_float(found[1]) if len(found) > 1 else None,
            "longitude": as_float(found[0]) if found else None,
            "logradouro_geocode": fold(dig(first, "properties", "text")),
            "numero_porta_geocode": None,
            "bairro_geocode": fold(names.get("neighborhood")),
            "cidade_geocode": fold(names.get("place")) or "rio de janeiro",
            "estado_geocode": fold(names.get("region")) or "rio de janeiro",
            "cep_geocode": (names.get("postcode") or "").strip() or None,
            "confianca": 1.0,
            "updated_date": today,
            "geocode": "maptiler",
        }

    return Provider("maptiler", build, parse)


def opencage(key: str) -> Provider:
    """Return OpenCage with a key.

    Its confidence goes from 0 to 10, and is brought to 0 to 1.
    """

    def build(address: str) -> Query:
        return Query(
            "https://api.opencagedata.com/geocode/v1/json", {"q": address, "key": key, "language": "pt", "limit": "1"}
        )

    def parse(response: Response, today: str) -> Geocode | None:
        if response.status_code == PAYMENT_REQUIRED:
            return None

        results = dig(read_json(response), "results")
        first = results[0] if isinstance(results, list) and results else None

        if not isinstance(first, dict):
            return None

        confidence = as_float(first.get("confidence"))
        components = dig(first, "components")

        return {
            "latitude": as_float(dig(first, "geometry", "lat")),
            "longitude": as_float(dig(first, "geometry", "lng")),
            "logradouro_geocode": fold(dig(components, "road")),
            "numero_porta_geocode": None,
            "bairro_geocode": fold(dig(components, "suburb")),
            "cidade_geocode": fold(dig(components, "city") or dig(components, "county")),
            "estado_geocode": fold(dig(components, "state")),
            "cep_geocode": text_of(dig(components, "postcode")),
            "confianca": (confidence or 0) / CONFIDENCE_SCALE,
            "updated_date": today,
            "geocode": "opencage",
        }

    return Provider("opencage", build, parse)


def locationiq(key: str) -> Provider:
    """Return LocationIQ with a key.

    Only a result whose city part of the name is Rio de Janeiro counts.
    """

    def build(address: str) -> Query:
        return Query("https://us1.locationiq.com/v1/search.php", {"key": key, "q": address, "format": "json"})

    def parse(response: Response, today: str) -> Geocode | None:
        if response.status_code == PAYMENT_REQUIRED:
            return None

        data = read_json(response)
        valid = [
            item
            for item in (data if isinstance(data, list) else [])
            if isinstance(item, dict)
            and RIO_CONTEXT in str(item.get("display_name", "")).split(", ")[LOCATIONIQ_CITY_PART]
        ]
        best = max(valid, key=lambda item: as_float(item.get("importance")) or 0, default=None)

        if best is None:
            return None

        parts = [unidecode(part.strip().lower()) for part in str(best["display_name"]).split(",")]

        return {
            "latitude": as_float(best.get("lat")) or 0.0,
            "longitude": as_float(best.get("lon")) or 0.0,
            "logradouro_geocode": parts[0],
            "numero_porta_geocode": None,
            "bairro_geocode": parts[1] if len(parts) > 1 else None,
            "cidade_geocode": parts[LOCATIONIQ_CITY_PART] if len(parts) > LOCATIONIQ_CITY_PART else None,
            "estado_geocode": parts[-LOCATIONIQ_STATE_PART] if len(parts) > LOCATIONIQ_STATE_PART else None,
            "cep_geocode": parts[-2] if len(parts) > LOCATIONIQ_CITY_PART else None,
            "confianca": as_float(best.get("importance")) or 0.0,
            "updated_date": today,
            "geocode": "locationiq",
        }

    return Provider("locationiq", build, parse)


def is_valid_xyz(data: JsonValue) -> bool:
    """Tell whether a Geocode.xyz answer has numeric, negative coordinates.

    Rio has negative coordinates.
    """
    latitude, longitude = as_float(dig(data, "latt")), as_float(dig(data, "longt"))

    return latitude is not None and longitude is not None and latitude < 0 and longitude < 0


def read_state(state: JsonValue) -> str | None:
    """Return the Geocode.xyz state, which is a text, or an object or a list.

    For a list, the first item counts.
    """
    if isinstance(state, str):
        return fold(state, strip=False)

    return text_of(next(iter(state), None)) if isinstance(state, dict | list) else None


def parse_xyz(data: JsonValue, today: str) -> Geocode:
    """Read an answer of Geocode.xyz that has valid coordinates."""
    standard = dig(data, "standard")
    state = dig(standard, "statename")

    return {
        "latitude": text_of(dig(data, "latt")),
        "longitude": text_of(dig(data, "longt")),
        "logradouro_geocode": fold(dig(standard, "addresst"), strip=False),
        "numero_porta_geocode": text_of(dig(standard, "stnumber")),
        "bairro_geocode": None,
        "cidade_geocode": fold(dig(standard, "city"), strip=False),
        "estado_geocode": read_state(state),
        "cep_geocode": text_of(dig(standard, "postal")),
        "confianca": as_float(dig(standard, "confidence")),
        "updated_date": today,
        "geocode": "geocode_xyz",
    }


def xyz_url(address: str, key: str) -> str:
    """Return the Geocode.xyz request address, with the key if there is one."""
    return f"https://geocode.xyz/{address}?json=1" + (f"&auth={key}" if key else "")


def finish_geoapify(item: JsonValue, today: str) -> Geocode | None:
    """Read a result of a Geoapify batch, or nothing if it has no coordinates."""
    if not isinstance(item, dict) or item.get("lat") is None or item.get("lon") is None:
        return None

    confidence = dig(item, "rank", "confidence")

    return {
        "latitude": as_float(item.get("lat")),
        "longitude": as_float(item.get("lon")),
        "logradouro_geocode": fold(item.get("street")),
        "numero_porta_geocode": text_of(item.get("housenumber")),
        "bairro_geocode": fold(item.get("suburb") or item.get("district")),
        "cidade_geocode": fold(item.get("city")),
        "estado_geocode": fold(item.get("state")),
        "cep_geocode": text_of(item.get("postcode")),
        "confianca": as_float(confidence),
        "updated_date": today,
        "geocode": "geoapify",
    }
