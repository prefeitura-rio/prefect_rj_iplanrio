# -*- coding: utf-8 -*-
"""
Helper functions for geocoding
"""

import random
from time import sleep, time

import pandas as pd
import pendulum
import requests
from geopy.geocoders import Nominatim
from iplanrio.pipelines_utils.infisical import get_secret
from iplanrio.pipelines_utils.logging import log
from openlocationcode import openlocationcode as olc
from unidecode import unidecode

GEOCODING_FIELDS = [
    "latitude",
    "longitude",
    "logradouro_geocode",
    "numero_porta_geocode",
    "bairro_geocode",
    "cidade_geocode",
    "estado_geocode",
    "cep_geocode",
    "confianca",
    "updated_date",
]

DEFAULT_SLEEP_TIME = 1.1
DEFAULT_TIMEOUT = 10
MAX_RETRIES_DEFAULT = 3
RETRY_WAIT_TIME_DEFAULT = 150
BASE_BACKOFF_TIME = 1.0
MAX_BACKOFF_TIME = 60.0
JITTER_RANGE = 0.1


def _prepare_addresses_for_nominatim(addresses: list[str]) -> list[str]:
    """Prepare addresses for Nominatim geocoding by ensuring Rio de Janeiro context."""
    return [f"{address}, Rio de Janeiro" if "Rio de Janeiro" not in address else address for address in addresses]


def _create_empty_geocoding_result(updated_date: str) -> list:
    """Create an empty geocoding result with None values."""
    return [None] * (len(GEOCODING_FIELDS) - 1) + [updated_date]


def _parse_nominatim_result(location, updated_date: str) -> list:
    """Parse Nominatim API response into standardized format."""
    if not location:
        return _create_empty_geocoding_result(updated_date)

    address_data = location.raw["address"]
    return [
        location.raw["lat"],
        location.raw["lon"],
        unidecode(address_data.get("road", "")).lower(),
        address_data.get("house_number"),
        unidecode(address_data.get("suburb", "")).lower(),
        unidecode(address_data.get("city", "")).lower(),
        unidecode(address_data.get("state", "")).lower(),
        address_data.get("postcode"),
        None,
        updated_date,
    ]


def geocode_nominatim(
    new_addresses: pd.DataFrame,
    address_column: str = "address",
    log_divider: int = 60,
    return_original_cols: bool = False,
    use_exponential_backoff: bool = True,
) -> pd.DataFrame:
    """Geocode addresses using Nominatim API."""
    log("Start georeferencing dataframe")
    start_time = time()

    all_addresses = new_addresses[address_column].tolist()
    all_addresses = _prepare_addresses_for_nominatim(all_addresses)

    user_agent = get_secret("NOMINATIM", path="/nominatim")["NOMINATIM"]
    domain = get_secret("NOMINATIM_DOMAIN", path="/nominatim")["NOMINATIM_DOMAIN"]
    geolocator = Nominatim(user_agent=user_agent, domain=domain)

    log(f"There are {len(all_addresses)} addresses to georeference")
    updated_date = pendulum.now(tz="America/Sao_Paulo").to_date_string()
    locations = []
    consecutive_errors = 0

    for i, address in enumerate(all_addresses):
        if i % log_divider == 0:
            log(f"Georeferencing address {i} of {len(all_addresses)}...")

        try:
            cleaned_address = address.replace(" RJ,", "").replace("Brasil", "Brazil")
            location = geolocator.geocode(cleaned_address, country_codes=["br"], addressdetails=True)
            geolocated_addresses = _parse_nominatim_result(location, updated_date)
            consecutive_errors = 0
        except Exception as e:
            log(f"Error georeferencing address {i} - {address}: {e}")
            geolocated_addresses = _create_empty_geocoding_result(updated_date)
            consecutive_errors += 1

            if use_exponential_backoff and consecutive_errors > 1:
                exponential_backoff_sleep(consecutive_errors - 1)

        locations.append(geolocated_addresses)

        if use_exponential_backoff:
            exponential_backoff_sleep(0, 1.1)

    output = pd.DataFrame(locations, columns=GEOCODING_FIELDS)
    output[address_column] = new_addresses[address_column]

    if return_original_cols:
        output_cols = [col for col in output.columns if col != address_column]
        new_addresses[output_cols] = output[output_cols]
        output = new_addresses

    log(f"End georeferrencing dataframe in {(time() - start_time)} seconds")
    output["geocode"] = "nominatim"
    return output


def _is_real_number(value) -> bool:
    """Check if value is a real number."""
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False


def _build_geocode_xyz_url(base_url: str, address: str, api_key: str = None) -> str:
    """Build URL for Geocode.xyz API request."""
    url = f"{base_url}/{address}?json=1"
    if api_key:
        url += f"&auth={api_key}"
    return url


def _handle_geocode_xyz_throttling(url: str, attempt: int = 0) -> dict:
    """Handle API throttling for Geocode.xyz."""
    log("Throttled! Waiting before retrying...")
    exponential_backoff_sleep(attempt, 2.0)
    response = requests.get(url, timeout=DEFAULT_TIMEOUT)
    return response.json()


def _parse_geocode_xyz_result(data: dict, index: int, data_store: dict) -> None:
    """Parse Geocode.xyz API response into data store."""
    if not _is_valid_geocode_xyz_coordinates(data):
        return

    standard = data.get("standard", {})
    data_store["latitude"][index] = data["latt"]
    data_store["longitude"][index] = data["longt"]
    data_store["logradouro_geocode"][index] = (
        unidecode(standard.get("addresst", "")).lower() if standard.get("addresst") else None
    )
    data_store["numero_porta_geocode"][index] = standard.get("stnumber")
    data_store["bairro_geocode"][index] = None
    data_store["cidade_geocode"][index] = unidecode(standard.get("city", "")).lower() if standard.get("city") else None

    estado_geocode = standard.get("statename")
    if isinstance(estado_geocode, str):
        estado_geocode = unidecode(estado_geocode).lower()
    elif estado_geocode:
        estado_geocode = next(iter(estado_geocode), None)

    data_store["estado_geocode"][index] = estado_geocode
    data_store["cep_geocode"][index] = standard.get("postal")
    data_store["confianca"][index] = standard.get("confidence")
    data_store["updated_date"][index] = pd.Timestamp.now().strftime("%Y-%m-%d")


def _is_valid_geocode_xyz_coordinates(data: dict) -> bool:
    """Check if Geocode.xyz response contains valid coordinates."""
    return (
        "latt" in data
        and "longt" in data
        and _is_real_number(data["latt"])
        and float(data["latt"]) < 0
        and float(data["longt"]) < 0
    )


def _check_credit_exhaustion(
    data: dict, retry_count: int, max_retries: int, retry_wait_time: int, current_index: int
) -> tuple[bool, int, int]:
    """Check if API credits are exhausted and handle retry logic."""
    remaining_credits = data.get("remaining_credits", 1)
    if remaining_credits is not None and float(remaining_credits) <= 0:
        retry_count += 1
        log(f"No more credits for Geocode XYZ. Retry {retry_count}/{max_retries}")

        if retry_count < max_retries:
            log(f"Waiting {retry_wait_time} seconds before retrying...")
            sleep(retry_wait_time)
            return True, retry_count, current_index
        else:
            log("Maximum retries reached. Stopping geocoding.")
            return False, retry_count, current_index

    return False, retry_count, current_index


def geocode_geocodexyz(
    dataframe: pd.DataFrame,
    address_column: str,
    sleep_time: float = 1.6,
    api_key: str = None,
    max_retries: int = MAX_RETRIES_DEFAULT,
    retry_wait_time: int = RETRY_WAIT_TIME_DEFAULT,
    use_exponential_backoff: bool = True,
) -> pd.DataFrame:
    """Geocode addresses using Geocode.xyz API."""
    base_url = "https://geocode.xyz"
    data_store = {field: [None] * len(dataframe) for field in GEOCODING_FIELDS}
    retry_count = 0
    start_index = 0

    while retry_count < max_retries:
        should_break = False

        for i, address in enumerate(dataframe[address_column].iloc[start_index:], start=start_index):
            try:
                url = _build_geocode_xyz_url(base_url, address, api_key)
                response = requests.get(url, timeout=DEFAULT_TIMEOUT)

                if response.status_code == 402:  # Quota excedida
                    log("Lim. de reqs. atingido no Geocode.xyz. Saltando para próximo provider.")
                    raise requests.exceptions.HTTPError("402 Payment Required - quota exceeded")

                if response.status_code not in (429, 200):
                    continue

                data = response.json()

                if response.status_code == 429 or data.get("latt", "").startswith("Throttled"):
                    data = _handle_geocode_xyz_throttling(url, retry_count)

                if response.status_code == 200 and data.get("error"):
                    should_retry, retry_count, start_index = _check_credit_exhaustion(
                        data, retry_count, max_retries, retry_wait_time, i
                    )
                    if should_retry:
                        should_break = True
                        break
                    elif retry_count >= max_retries:
                        should_break = True
                        break

                _parse_geocode_xyz_result(data, i, data_store)

                if use_exponential_backoff:
                    exponential_backoff_sleep(0, sleep_time)
                else:
                    sleep(sleep_time)

            except Exception as e:
                log(f"Error geocoding {address} with Geocode.xyz: {e}")
                continue

        if should_break or start_index >= len(dataframe):
            break

    for key, values in data_store.items():
        if key not in dataframe.columns:
            dataframe[key] = None
        dataframe[key] = values

    dataframe["geocode"] = "geocode_xyz"
    successful_geocodes = dataframe.dropna(subset=["latitude"]).shape[0]
    log(f"Found {successful_geocodes} addresses of {dataframe.shape[0]} using Geocode XYZ")
    return dataframe


def geocode_locationiq(
    dataframe: pd.DataFrame,
    address_column: str,
    api_key: str,
    sleep_time: float = DEFAULT_SLEEP_TIME,
    use_exponential_backoff: bool = True,
) -> pd.DataFrame:
    """Geocode addresses using LocationIQ API."""

    dataframe = dataframe.copy()
    for field in GEOCODING_FIELDS:
        dataframe[field] = None

    # Obtém o número de requisições restantes na API
    try:
        balance_response = requests.get(f"https://us1.locationiq.com/v1/balance?key={api_key}&format=json").json()
        num_requests_left = int(balance_response.get("balance", {}).get("day", 0))
    except Exception as e:
        print(f"Erro ao verificar o saldo de requisições: {e}")
        return dataframe

    if num_requests_left == 0:
        print("Sem requisições restantes na API do LocationIQ.")
        return dataframe

    session = requests.Session()
    updated_date = pendulum.now(tz="America/Sao_Paulo").to_date_string()

    for idx, endereco in dataframe.iloc[:num_requests_left][address_column].items():
        url = f"https://us1.locationiq.com/v1/search.php?key={api_key}&q={endereco}&format=json"

        try:
            response = session.get(url)

            if response.status_code == 402:  # Quota excedida
                raise requests.exceptions.HTTPError("402 Payment Required - quota exceeded")

            data = response.json()

            if isinstance(data, list) and data:
                valid_results = [res for res in data if "Rio de Janeiro" in res.get("display_name", "").split(", ")[2]]
                best_result = max(valid_results, key=lambda x: x.get("importance", 0), default=None)

                if best_result:
                    display_parts = [unidecode(p.strip().lower()) for p in best_result["display_name"].split(",")]

                    dataframe.at[idx, "latitude"] = float(best_result.get("lat", 0))
                    dataframe.at[idx, "longitude"] = float(best_result.get("lon", 0))
                    dataframe.at[idx, "confianca"] = float(best_result.get("importance", 0))
                    dataframe.at[idx, "logradouro_geocode"] = display_parts[0]
                    dataframe.at[idx, "bairro_geocode"] = display_parts[1]
                    dataframe.at[idx, "cidade_geocode"] = display_parts[2]
                    dataframe.at[idx, "estado_geocode"] = display_parts[-4]
                    dataframe.at[idx, "cep_geocode"] = display_parts[-2]
                    dataframe.at[idx, "updated_date"] = updated_date

                else:
                    print(f"Endereço não encontrado: {endereco}")
            else:
                print(f"Endereço não encontrado: {endereco}")

        except Exception as e:
            print(f"Erro ao processar '{endereco}': {e}")

        if use_exponential_backoff:
            exponential_backoff_sleep(0, sleep_time)
        else:
            sleep(sleep_time)
    dataframe["geocode"] = "locationiq"
    return dataframe


def geocode_opencage(
    dataframe: pd.DataFrame,
    address_column: str,
    api_key: str,
    sleep_time: float = DEFAULT_SLEEP_TIME,
    use_exponential_backoff: bool = True,
) -> pd.DataFrame:
    """Geocode addresses using OpenCage API."""

    data_store = {field: [] for field in GEOCODING_FIELDS}

    url = "https://api.opencagedata.com/geocode/v1/json"

    for i, address in enumerate(dataframe[address_column]):
        params = {"q": address, "key": api_key, "language": "pt", "limit": 1}

        try:
            response = requests.get(url, params=params)

            if response.status_code == 402:  # Quota excedida
                raise requests.exceptions.HTTPError("402 Payment Required - quota exceeded")

            data = response.json()

            if data.get("results"):
                result = data["results"][0]
                components = result.get("components", {})

                data_store["latitude"].append(result["geometry"].get("lat"))
                data_store["longitude"].append(result["geometry"].get("lng"))
                data_store["logradouro_geocode"].append(unidecode(components.get("road", "").strip().lower()))
                data_store["numero_porta_geocode"].append(None)
                data_store["bairro_geocode"].append(unidecode(components.get("suburb", "").strip().lower()))
                if "city" in components:
                    data_store["cidade_geocode"].append(unidecode(components.get("city", "").strip().lower()))
                else:
                    data_store["cidade_geocode"].append(unidecode(components.get("county", "").strip().lower()))
                data_store["estado_geocode"].append(unidecode(components.get("state", "").strip().lower()))
                data_store["cep_geocode"].append(components.get("postcode", "").strip())
                data_store["confianca"].append(result.get("confidence", 0) / 10)
                data_store["updated_date"].append(pd.Timestamp.now().strftime("%Y-%m-%d"))
            else:
                log(f"Endereço '{address}' não encontrado.")
                for field in GEOCODING_FIELDS:
                    data_store[field].append(None)

        except requests.RequestException as e:
            log(f"Erro ao processar '{address}': {e}")
            for field in GEOCODING_FIELDS:
                data_store[field].append(None)

        if use_exponential_backoff:
            exponential_backoff_sleep(0, sleep_time)
        else:
            sleep(sleep_time)

    for field in GEOCODING_FIELDS:
        dataframe[field] = data_store[field]
    dataframe["geocode"] = "opencage"

    return dataframe


def geocode_maptiler(
    dataframe: pd.DataFrame,
    address_column: str,
    api_key: str,
    sleep_time: float = DEFAULT_SLEEP_TIME,
    use_exponential_backoff: bool = True,
) -> pd.DataFrame:
    """Geocode addresses using MapTiler API."""

    data_store = {field: [] for field in GEOCODING_FIELDS}

    base_url = "https://api.maptiler.com/geocoding"

    for i, address in enumerate(dataframe[address_column]):
        # Prepare address for MapTiler (include Rio de Janeiro context)
        search_address = (
            f"{address}, Rio de Janeiro, Brazil" if "Rio de Janeiro" not in address else f"{address}, Brazil"
        )

        url = f"{base_url}/{search_address}.json"
        params = {"key": api_key, "language": "pt", "limit": 1, "country": "br"}

        try:
            response = requests.get(url, params=params, timeout=DEFAULT_TIMEOUT)

            if response.status_code == 402:  # Quota excedida
                raise requests.exceptions.HTTPError("402 Payment Required - quota exceeded")

            if response.status_code == 429:  # Rate limit
                raise requests.exceptions.HTTPError("429 Too Many Requests - rate limited")

            data = response.json()

            if data.get("features") and len(data["features"]) > 0:
                result = data["features"][0]
                geometry = result.get("geometry", {})
                properties = result.get("properties", {})

                # Extract coordinates
                coordinates = geometry.get("coordinates", [])
                lon = coordinates[0] if len(coordinates) > 0 else None
                lat = coordinates[1] if len(coordinates) > 1 else None

                data_store["latitude"].append(lat)
                data_store["longitude"].append(lon)

                # Parse address components from properties
                place_name = properties.get("place_name", "")
                context = properties.get("context", [])

                # Extract street from text or place_name
                street = properties.get("text", "")
                if street:
                    data_store["logradouro_geocode"].append(unidecode(street).lower().strip())
                else:
                    data_store["logradouro_geocode"].append(None)

                # Extract neighborhood/district from context
                neighborhood = None
                city = None
                state = None
                postcode = None

                for ctx in context:
                    ctx_type = ctx.get("id", "").split(".")[0]
                    if ctx_type in ["neighborhood", "locality"]:
                        neighborhood = ctx.get("text", "")
                    elif ctx_type == "place":
                        city = ctx.get("text", "")
                    elif ctx_type == "region":
                        state = ctx.get("text", "")
                    elif ctx_type == "postcode":
                        postcode = ctx.get("text", "")

                data_store["numero_porta_geocode"].append(None)
                data_store["bairro_geocode"].append(unidecode(neighborhood).lower().strip() if neighborhood else None)
                data_store["cidade_geocode"].append(unidecode(city).lower().strip() if city else "rio de janeiro")
                data_store["estado_geocode"].append(unidecode(state).lower().strip() if state else "rio de janeiro")
                data_store["cep_geocode"].append(postcode.strip() if postcode else None)

                # MapTiler doesn't provide explicit confidence score, use 1.0 for successful matches
                data_store["confianca"].append(1.0)
                data_store["updated_date"].append(pd.Timestamp.now().strftime("%Y-%m-%d"))
            else:
                # No results found
                for field in GEOCODING_FIELDS:
                    data_store[field].append(None)

        except Exception as e:
            log(f"Error geocoding address '{address}' with MapTiler: {e}")
            for field in GEOCODING_FIELDS:
                data_store[field].append(None)

        # Rate limiting - sleep between requests
        if use_exponential_backoff:
            exponential_backoff_sleep(0, sleep_time)
        else:
            sleep(sleep_time)

    # Create output dataframe
    output = dataframe.copy()
    for field in GEOCODING_FIELDS:
        output[field] = data_store[field]
    output["geocode"] = "maptiler"

    successful_geocodes = output.dropna(subset=["latitude"]).shape[0]
    log(f"Found {successful_geocodes} addresses of {output.shape[0]} using MapTiler")

    return output


def coordinates_to_pluscode(latitude: float, longitude: float) -> str | None:
    """
    Converts latitude and longitude coordinates to a Google Plus Code.

    Args:
        latitude (float): The latitude coordinate (-90 to 90)
        longitude (float): The longitude coordinate (-180 to 180)

    Returns:
        str | None: The Plus Code representation of the coordinates,
        or None if coordinates are invalid

    Raises:
        ValueError: If the coordinates are outside valid ranges or not numeric
        TypeError: If the coordinates are not numeric types

    Example:
        >>> coordinates_to_pluscode(-22.907104, -43.172897)
        '58G8P27V+JG'
    """

    try:
        latitude = float(latitude)
        longitude = float(longitude)
    except (ValueError, TypeError):
        return None

    if not -90 <= latitude <= 90:
        log(f"Latitude {latitude} is not between -90 and 90")
        return None
    if not -180 <= longitude <= 180:
        log(f"Longitude {longitude} is not between -180 and 180")
        return None

    return olc.encode(latitude, longitude)


def exponential_backoff_sleep(attempt: int, base_time: float = BASE_BACKOFF_TIME) -> None:
    """
    Sleep with exponential backoff and jitter.

    Args:
        attempt (int): Current attempt number (0-indexed)
        base_time (float): Base sleep time in seconds
    """
    backoff_time = min(base_time * (2**attempt), MAX_BACKOFF_TIME)
    jitter = random.uniform(-JITTER_RANGE, JITTER_RANGE) * backoff_time
    sleep_time = max(0.1, backoff_time + jitter)
    sleep(sleep_time)


def check_if_belongs_to_rio(lat: float, long: float) -> list:
    """
    Verifica se o lat/long retornado pela API pertence ao geometry
    da cidade do Rio de Janeiro. Se pertencer retorna o lat, lon, se não retorna None.

    Args:
        lat (float): Latitude
        long (float): Longitude

    Returns:
        list: [lat, long] se pertence ao Rio, [None, None] caso contrário
    """
    # TODO: Implementar verificação geográfica usando shapely/geopandas
    # Por enquanto retorna os valores originais
    # Exemplo futuro:
    # from shapely.geometry import Point
    # from basedosdados import read_municipality
    #
    # rio = read_municipality(code_muni=3304557, year=2020)
    # point = Point(long, lat)
    # polygon = rio.geometry
    # pertence = polygon.contains(point)
    #
    # if pertence.iloc[0]:
    #     return [lat, long]
    # else:
    #     return [None, None]

    return [lat, long]
