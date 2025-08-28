# -*- coding: utf-8 -*-
"""
Async helper functions for geocoding
"""

import asyncio
import random
from time import time
from typing import Any, Dict, Optional

import aiohttp
import pandas as pd
import pendulum
from iplanrio.pipelines_utils.logging import log
from unidecode import unidecode

from .geo_utils import (
    BASE_BACKOFF_TIME,
    DEFAULT_SLEEP_TIME,
    DEFAULT_TIMEOUT,
    GEOCODING_FIELDS,
    JITTER_RANGE,
    MAX_BACKOFF_TIME,
    MAX_RETRIES_DEFAULT,
    _create_empty_geocoding_result,
    _prepare_addresses_for_nominatim,
)


async def _async_exponential_backoff_sleep(attempt: int, base_time: float = BASE_BACKOFF_TIME) -> None:
    """
    Async sleep with exponential backoff and jitter.

    Args:
        attempt (int): Current attempt number (0-indexed)
        base_time (float): Base sleep time in seconds
    """
    backoff_time = min(base_time * (2**attempt), MAX_BACKOFF_TIME)
    jitter = random.uniform(-JITTER_RANGE, JITTER_RANGE) * backoff_time
    sleep_time = max(0.1, backoff_time + jitter)
    await asyncio.sleep(sleep_time)


def _create_empty_async_result(address: str, updated_date: str, provider: str = "opencage") -> Dict[str, Any]:
    """Create empty geocoding result for async functions."""
    return {
        "address": address,
        "latitude": None,
        "longitude": None,
        "logradouro_geocode": None,
        "numero_porta_geocode": None,
        "bairro_geocode": None,
        "cidade_geocode": None,
        "estado_geocode": None,
        "cep_geocode": None,
        "confianca": None,
        "updated_date": updated_date,
        "geocode": provider,
    }


def _log_progress(
    geocoder_name: str, processed: int, total: int, successes: int, failures: int, interval: int = 100
) -> None:
    """Log progress for geocoding operations at specified intervals."""
    if processed % interval == 0 or processed == total:
        success_rate = (successes / processed * 100) if processed > 0 else 0
        log(
            f"[{geocoder_name}] Processados: {processed}/{total} endereços "
            f"({successes} sucessos, {failures} falhas) - {success_rate:.1f}% sucesso"
        )


async def _geocode_single_nominatim_async(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    address: str,
    user_agent: str,
    domain: str,
    sleep_time: float,
    use_exponential_backoff: bool,
) -> Dict[str, Any]:
    """Geocode a single address using Nominatim API asynchronously."""
    async with semaphore:
        updated_date = pendulum.now(tz="America/Sao_Paulo").to_date_string()

        try:
            # Prepare address for Nominatim using utility function
            prepared_addresses = _prepare_addresses_for_nominatim([address])
            cleaned_address = prepared_addresses[0].replace(" RJ,", "").replace("Brasil", "Brazil")

            # Build request URL
            params = {
                "q": cleaned_address,
                "format": "json",
                "addressdetails": "1",
                "countrycodes": "br",
                "limit": "1",
            }

            headers = {"User-Agent": user_agent}

            async with session.get(
                f"https://{domain}/search",
                params=params,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=DEFAULT_TIMEOUT),
            ) as response:
                if response.status != 200:
                    log(f"Nominatim request failed with status {response.status} for address: {address}")
                    # Use utility function for empty result
                    empty_result = _create_empty_geocoding_result(updated_date)
                    geocode_result = dict(zip(GEOCODING_FIELDS, empty_result, strict=False))
                    geocode_result["address"] = address
                    geocode_result["geocode"] = "nominatim"
                    return geocode_result

                data = await response.json()

                if data and len(data) > 0:
                    location_data = data[0]
                    address_data = location_data.get("address", {})

                    # Parse result manually since _parse_nominatim_result expects geopy object
                    geocode_result = {
                        "address": address,
                        "latitude": location_data.get("lat"),
                        "longitude": location_data.get("lon"),
                        "logradouro_geocode": (
                            unidecode(address_data.get("road", "")).lower()
                            if address_data.get("road")
                            else None
                        ),
                        "numero_porta_geocode": address_data.get("house_number"),
                        "bairro_geocode": (
                            unidecode(address_data.get("suburb", "")).lower()
                            if address_data.get("suburb")
                            else None
                        ),
                        "cidade_geocode": (
                            unidecode(address_data.get("city", "")).lower()
                            if address_data.get("city")
                            else None
                        ),
                        "estado_geocode": (
                            unidecode(address_data.get("state", "")).lower()
                            if address_data.get("state")
                            else None
                        ),
                        "cep_geocode": address_data.get("postcode"),
                        "confianca": None,
                        "updated_date": updated_date,
                        "geocode": "nominatim",
                    }
                else:
                    # Use utility function for empty result
                    empty_result = _create_empty_geocoding_result(updated_date)
                    geocode_result = dict(zip(GEOCODING_FIELDS, empty_result, strict=False))
                    geocode_result["address"] = address
                    geocode_result["geocode"] = "nominatim"

        except Exception:
            # Use utility function for empty result
            empty_result = _create_empty_geocoding_result(updated_date)
            geocode_result = dict(zip(GEOCODING_FIELDS, empty_result, strict=False))
            geocode_result["address"] = address
            geocode_result["geocode"] = "nominatim"

    # Sleep outside semaphore to allow true concurrency
    if use_exponential_backoff:
        await _async_exponential_backoff_sleep(0, sleep_time)
    else:
        await asyncio.sleep(sleep_time)

    return geocode_result


async def _geocode_single_waze_async(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    address: str,
    sleep_time: float,
    use_exponential_backoff: bool,
) -> Dict[str, Any]:
    """Geocode a single address using Waze Autocomplete API asynchronously."""
    async with semaphore:
        base_url = "https://www.waze.com/live-map/api/autocomplete/"
        params = {
            "q": address,
            "exp": "8,10,12",
            "geo-env": "row",
            "v": "-22.93958242,-43.198843;-22.87404002,-43.1470871",
            "lang": "pt-BR",
        }
        updated_date = pd.Timestamp.now().strftime("%Y-%m-%d")

        try:
            async with session.get(
                base_url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=DEFAULT_TIMEOUT),
            ) as response:
                if response.status == 429:
                    geocode_result = _create_empty_async_result(address, updated_date, "waze")
                elif response.status != 200:
                    geocode_result = _create_empty_async_result(address, updated_date, "waze")
                else:
                    data = await response.json()

                    if data and len(data) > 0:
                        # Get the first result
                        result = data[0]
                        location = result.get("location", {})

                        lat = location.get("lat")
                        lon = location.get("lon")

                        # Parse address components from result
                        name = result.get("name", "")
                        full_name = result.get("fullName", "")

                        # Extract components from fullName or name
                        address_parts = full_name.split(", ") if full_name else name.split(", ")

                        street = address_parts[0] if len(address_parts) > 0 else ""
                        neighborhood = address_parts[1] if len(address_parts) > 1 else ""
                        city = address_parts[2] if len(address_parts) > 2 else "Rio de Janeiro"

                        geocode_result = {
                            "address": address,
                            "latitude": lat,
                            "longitude": lon,
                            "logradouro_geocode": unidecode(street).lower().strip() if street else None,
                            "numero_porta_geocode": None,
                            "bairro_geocode": unidecode(neighborhood).lower().strip() if neighborhood else None,
                            "cidade_geocode": unidecode(city).lower().strip() if city else "rio de janeiro",
                            "estado_geocode": "rio de janeiro",
                            "cep_geocode": None,
                            "confianca": 1.0,
                            "updated_date": updated_date,
                            "geocode": "waze",
                        }
                    else:
                        geocode_result = _create_empty_async_result(address, updated_date, "waze")

        except Exception:
            geocode_result = _create_empty_async_result(address, updated_date, "waze")

    # Sleep outside semaphore to allow true concurrency
    if use_exponential_backoff:
        await _async_exponential_backoff_sleep(0, sleep_time)
    else:
        await asyncio.sleep(sleep_time)

    return geocode_result


async def _geocode_single_maptiler_async(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    address: str,
    api_key: str,
    sleep_time: float,
    use_exponential_backoff: bool,
) -> Dict[str, Any]:
    """Geocode a single address using MapTiler API asynchronously."""
    async with semaphore:
        # Prepare address for MapTiler (include Rio de Janeiro context)
        search_address = (
            f"{address}, Rio de Janeiro, Brazil" if "Rio de Janeiro" not in address else f"{address}, Brazil"
        )

        url = f"https://api.maptiler.com/geocoding/{search_address}.json"
        params = {"key": api_key, "language": "pt", "limit": 1, "country": "br"}
        updated_date = pd.Timestamp.now().strftime("%Y-%m-%d")

        try:
            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=DEFAULT_TIMEOUT),
            ) as response:
                if response.status == 402:
                    geocode_result = _create_empty_async_result(address, updated_date, "maptiler")
                elif response.status == 429:
                    geocode_result = _create_empty_async_result(address, updated_date, "maptiler")
                else:
                    data = await response.json()

                    if data.get("features") and len(data["features"]) > 0:
                        result = data["features"][0]
                        geometry = result.get("geometry", {})
                        properties = result.get("properties", {})

                        # Extract coordinates
                        coordinates = geometry.get("coordinates", [])
                        lon = coordinates[0] if len(coordinates) > 0 else None
                        lat = coordinates[1] if len(coordinates) > 1 else None

                        # Parse address components from properties
                        context = properties.get("context", [])

                        # Extract street from text or place_name
                        street = properties.get("text", "")

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

                        geocode_result = {
                            "address": address,
                            "latitude": lat,
                            "longitude": lon,
                            "logradouro_geocode": unidecode(street).lower().strip() if street else None,
                            "numero_porta_geocode": None,
                            "bairro_geocode": unidecode(neighborhood).lower().strip() if neighborhood else None,
                            "cidade_geocode": unidecode(city).lower().strip() if city else "rio de janeiro",
                            "estado_geocode": unidecode(state).lower().strip() if state else "rio de janeiro",
                            "cep_geocode": postcode.strip() if postcode else None,
                            "confianca": 1.0,  # MapTiler doesn't provide explicit confidence
                            "updated_date": updated_date,
                            "geocode": "maptiler",
                        }
                    else:
                        geocode_result = _create_empty_async_result(address, updated_date, "maptiler")

        except Exception:
            geocode_result = _create_empty_async_result(address, updated_date, "maptiler")

    # Sleep outside semaphore to allow true concurrency
    if use_exponential_backoff:
        await _async_exponential_backoff_sleep(0, sleep_time)
    else:
        await asyncio.sleep(sleep_time)

    return geocode_result


async def _geocode_single_opencage_async(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    address: str,
    api_key: str,
    sleep_time: float,
    use_exponential_backoff: bool,
) -> Dict[str, Any]:
    """Geocode a single address using OpenCage API asynchronously."""
    async with semaphore:
        params = {"q": address, "key": api_key, "language": "pt", "limit": 1}
        updated_date = pd.Timestamp.now().strftime("%Y-%m-%d")

        try:
            async with session.get(
                "https://api.opencagedata.com/geocode/v1/json",
                params=params,
                timeout=aiohttp.ClientTimeout(total=DEFAULT_TIMEOUT),
            ) as response:
                if response.status == 402:
                    geocode_result = _create_empty_async_result(address, updated_date, "opencage")
                else:
                    data = await response.json()

                    if data.get("results"):
                        result = data["results"][0]
                        components = result.get("components", {})

                        geocode_result = {
                            "address": address,
                            "latitude": result["geometry"].get("lat"),
                            "longitude": result["geometry"].get("lng"),
                            "logradouro_geocode": (
                                unidecode(components.get("road", "").strip().lower())
                                if components.get("road")
                                else None
                            ),
                            "numero_porta_geocode": None,
                            "bairro_geocode": (
                                unidecode(components.get("suburb", "").strip().lower())
                                if components.get("suburb")
                                else None
                            ),
                            "cidade_geocode": (
                                unidecode(
                                    components.get("city", components.get("county", "")).strip().lower()
                                )
                                if components.get("city") or components.get("county")
                                else None
                            ),
                            "estado_geocode": (
                                unidecode(components.get("state", "").strip().lower())
                                if components.get("state")
                                else None
                            ),
                            "cep_geocode": (
                                components.get("postcode", "").strip() if components.get("postcode") else None
                            ),
                            "confianca": result.get("confidence", 0) / 10,
                            "updated_date": updated_date,
                            "geocode": "opencage",
                        }
                    else:
                        geocode_result = _create_empty_async_result(address, updated_date, "opencage")

        except Exception:
            geocode_result = _create_empty_async_result(address, updated_date, "opencage")

    # Sleep outside semaphore to allow true concurrency
    if use_exponential_backoff:
        await _async_exponential_backoff_sleep(0, sleep_time)
    else:
        await asyncio.sleep(sleep_time)

    return geocode_result


async def _geocode_single_geocodexyz_async(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    address: str,
    api_key: Optional[str],
    sleep_time: float,
    use_exponential_backoff: bool,
    max_retries: int = MAX_RETRIES_DEFAULT,
) -> Dict[str, Any]:
    """Geocode a single address using Geocode.xyz API asynchronously."""
    from .geo_utils import _build_geocode_xyz_url, _is_valid_geocode_xyz_coordinates

    async with semaphore:
        base_url = "https://geocode.xyz"
        url = _build_geocode_xyz_url(base_url, address, api_key)
        updated_date = pd.Timestamp.now().strftime("%Y-%m-%d")

        for retry in range(max_retries):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=DEFAULT_TIMEOUT)) as response:
                    if response.status == 402:
                        # Don't raise exception, just break to return empty result
                        break

                    if response.status not in (200, 429):
                        continue

                    data = await response.json()

                    if response.status == 429 or data.get("latt", "").startswith("Throttled"):
                        await _async_exponential_backoff_sleep(retry, 2.0)
                        continue

                    if response.status == 200 and data.get("error"):
                        remaining_credits = data.get("remaining_credits", 1)
                        if remaining_credits is not None and float(remaining_credits) <= 0:
                            # Don't retry when credits are exhausted - fail fast
                            break

                    if _is_valid_geocode_xyz_coordinates(data):
                        standard = data.get("standard", {})
                        geocode_result = {
                            "address": address,
                            "latitude": data["latt"],
                            "longitude": data["longt"],
                            "logradouro_geocode": (
                                unidecode(standard.get("addresst", "")).lower()
                                if standard.get("addresst")
                                else None
                            ),
                            "numero_porta_geocode": standard.get("stnumber"),
                            "bairro_geocode": None,
                            "cidade_geocode": (
                                unidecode(standard.get("city", "")).lower() if standard.get("city") else None
                            ),
                            "estado_geocode": None,
                            "cep_geocode": standard.get("postal"),
                            "confianca": standard.get("confidence"),
                            "updated_date": updated_date,
                            "geocode": "geocode_xyz",
                        }

                        if standard.get("statename"):
                            estado = standard["statename"]
                            if isinstance(estado, str):
                                geocode_result["estado_geocode"] = unidecode(estado).lower()
                            elif estado:
                                geocode_result["estado_geocode"] = next(iter(estado), None)

                        # Release semaphore before sleeping to allow true concurrency
                        result = geocode_result
                        break

            except Exception:
                if retry == max_retries - 1:
                    break
                continue

        # Only create empty result if no success was found
        if "result" not in locals():
            result = _create_empty_async_result(address, updated_date, "geocode_xyz")

    # Sleep outside semaphore to allow true concurrency
    if use_exponential_backoff:
        await _async_exponential_backoff_sleep(0, sleep_time)
    else:
        await asyncio.sleep(sleep_time)

    return result


async def _geocode_single_locationiq_async(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    address: str,
    api_key: str,
    sleep_time: float,
    use_exponential_backoff: bool,
) -> Dict[str, Any]:
    """Geocode a single address using LocationIQ API asynchronously."""
    async with semaphore:
        url = f"https://us1.locationiq.com/v1/search.php?key={api_key}&q={address}&format=json"
        updated_date = pendulum.now(tz="America/Sao_Paulo").to_date_string()

        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=DEFAULT_TIMEOUT)) as response:
                if response.status == 402:
                    log("LocationIQ rate limited")
                    geocode_result = _create_empty_async_result(address, updated_date, "locationiq")
                else:
                    data = await response.json()

                    if isinstance(data, list) and data:
                        valid_results = [
                            res
                            for res in data
                            if "Rio de Janeiro" in res.get("display_name", "").split(", ")[2]
                        ]
                        best_result = max(
                            valid_results, key=lambda x: x.get("importance", 0), default=None
                        )

                        if best_result:
                            display_parts = [
                                unidecode(p.strip().lower()) for p in best_result["display_name"].split(",")
                            ]

                            geocode_result = {
                                "address": address,
                                "latitude": float(best_result.get("lat", 0)),
                                "longitude": float(best_result.get("lon", 0)),
                                "logradouro_geocode": (display_parts[0] if len(display_parts) > 0 else None),
                                "numero_porta_geocode": None,
                                "bairro_geocode": (display_parts[1] if len(display_parts) > 1 else None),
                                "cidade_geocode": (display_parts[2] if len(display_parts) > 2 else None),
                                "estado_geocode": (display_parts[-4] if len(display_parts) > 4 else None),
                                "cep_geocode": (display_parts[-2] if len(display_parts) > 2 else None),
                                "confianca": float(best_result.get("importance", 0)),
                                "updated_date": updated_date,
                                "geocode": "locationiq",
                            }
                        else:
                            geocode_result = _create_empty_async_result(address, updated_date, "locationiq")
                    else:
                        geocode_result = _create_empty_async_result(address, updated_date, "locationiq")

        except Exception:
            geocode_result = _create_empty_async_result(address, updated_date, "locationiq")

    # Sleep outside semaphore to allow true concurrency
    if use_exponential_backoff:
        await _async_exponential_backoff_sleep(0, sleep_time)
    else:
        await asyncio.sleep(sleep_time)

    return geocode_result


async def geocode_nominatim_async(
    dataframe: pd.DataFrame,
    address_column: str,
    user_agent: str,
    domain: str,
    max_concurrent: int = 10,
    sleep_time: float = DEFAULT_SLEEP_TIME,
    use_exponential_backoff: bool = True,
    return_original_cols: bool = False,
) -> pd.DataFrame:
    """Geocode addresses using Nominatim API asynchronously."""
    log(f"Starting async geocoding of {len(dataframe)} addresses with Nominatim")
    start_time = time()

    addresses = dataframe[address_column].tolist()
    semaphore = asyncio.Semaphore(max_concurrent)

    async with aiohttp.ClientSession() as session:
        tasks = [
            _geocode_single_nominatim_async(
                session, semaphore, address, user_agent, domain, sleep_time, use_exponential_backoff
            )
            for address in addresses
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Convert results to DataFrame
    valid_results = [r for r in results if isinstance(r, dict)]
    result_df = pd.DataFrame(valid_results)

    # Merge with original dataframe
    output = dataframe.copy()
    for field in GEOCODING_FIELDS:
        output[field] = None

    if not result_df.empty:
        successes = 0
        failures = 0
        total_addresses = len(valid_results)

        for i, result in enumerate(valid_results):
            if i < len(output):
                # Check if this result has valid coordinates
                has_coords = result.get("latitude") is not None and result.get("longitude") is not None
                if has_coords:
                    successes += 1
                else:
                    failures += 1

                for field in GEOCODING_FIELDS:
                    if field in result:
                        output.at[i, field] = result[field]

                # Log progress every 100 addresses
                processed = i + 1
                _log_progress("Nominatim", processed, total_addresses, successes, failures)

        output["geocode"] = "nominatim"

    if return_original_cols:
        # Keep original columns and add geocoding results
        output_cols = [col for col in output.columns if col != address_column]
        final_output = dataframe.copy()
        final_output[output_cols] = output[output_cols]
        output = final_output

    log(f"Async Nominatim geocoding completed in {(time() - start_time): .2f} seconds")
    return output


async def geocode_waze_async(
    dataframe: pd.DataFrame,
    address_column: str,
    max_concurrent: int = 10,
    sleep_time: float = DEFAULT_SLEEP_TIME,
    use_exponential_backoff: bool = True,
) -> pd.DataFrame:
    """Geocode addresses using Waze Autocomplete API asynchronously."""
    log(f"Starting async geocoding of {len(dataframe)} addresses with Waze")
    start_time = time()

    addresses = dataframe[address_column].tolist()
    semaphore = asyncio.Semaphore(max_concurrent)

    async with aiohttp.ClientSession() as session:
        tasks = [
            _geocode_single_waze_async(session, semaphore, address, sleep_time, use_exponential_backoff)
            for address in addresses
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Convert results to DataFrame
    valid_results = [r for r in results if isinstance(r, dict)]
    result_df = pd.DataFrame(valid_results)

    # Merge with original dataframe
    output = dataframe.copy()
    for field in GEOCODING_FIELDS:
        output[field] = None

    if not result_df.empty:
        successes = 0
        failures = 0
        total_addresses = len(valid_results)

        for i, result in enumerate(valid_results):
            # Check if this result has valid coordinates
            has_coords = result.get("latitude") is not None and result.get("longitude") is not None
            if has_coords:
                successes += 1
            else:
                failures += 1

            for field in GEOCODING_FIELDS:
                if field in result:
                    output.at[i, field] = result[field]

            # Log progress every 100 addresses
            processed = i + 1
            _log_progress("Waze", processed, total_addresses, successes, failures)

        output["geocode"] = "waze"

    successful_geocodes = output.dropna(subset=["latitude"]).shape[0]
    log(f"Found {successful_geocodes} addresses of {output.shape[0]} using async Waze")
    log(f"Async Waze geocoding completed in {(time() - start_time): .2f} seconds")
    return output


async def geocode_maptiler_async(
    dataframe: pd.DataFrame,
    address_column: str,
    api_key: str,
    max_concurrent: int = 10,
    sleep_time: float = DEFAULT_SLEEP_TIME,
    use_exponential_backoff: bool = True,
) -> pd.DataFrame:
    """Geocode addresses using MapTiler API asynchronously."""
    log(f"Starting async geocoding of {len(dataframe)} addresses with MapTiler")
    start_time = time()

    addresses = dataframe[address_column].tolist()
    semaphore = asyncio.Semaphore(max_concurrent)

    async with aiohttp.ClientSession() as session:
        tasks = [
            _geocode_single_maptiler_async(session, semaphore, address, api_key, sleep_time, use_exponential_backoff)
            for address in addresses
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Convert results to DataFrame
    valid_results = [r for r in results if isinstance(r, dict)]
    result_df = pd.DataFrame(valid_results)

    # Merge with original dataframe
    output = dataframe.copy()
    for field in GEOCODING_FIELDS:
        output[field] = None

    if not result_df.empty:
        successes = 0
        failures = 0
        total_addresses = len(valid_results)

        for i, result in enumerate(valid_results):
            # Check if this result has valid coordinates
            has_coords = result.get("latitude") is not None and result.get("longitude") is not None
            if has_coords:
                successes += 1
            else:
                failures += 1

            for field in GEOCODING_FIELDS:
                if field in result:
                    output.at[i, field] = result[field]

            # Log progress every 100 addresses
            processed = i + 1
            _log_progress("MapTiler", processed, total_addresses, successes, failures)

        output["geocode"] = "maptiler"

    successful_geocodes = output.dropna(subset=["latitude"]).shape[0]
    log(f"Found {successful_geocodes} addresses of {output.shape[0]} using async MapTiler")
    log(f"Async MapTiler geocoding completed in {(time() - start_time): .2f} seconds")
    return output


async def geocode_opencage_async(
    dataframe: pd.DataFrame,
    address_column: str,
    api_key: str,
    max_concurrent: int = 10,
    sleep_time: float = DEFAULT_SLEEP_TIME,
    use_exponential_backoff: bool = True,
) -> pd.DataFrame:
    """Geocode addresses using OpenCage API asynchronously."""
    log(f"Starting async geocoding of {len(dataframe)} addresses with OpenCage")
    start_time = time()

    addresses = dataframe[address_column].tolist()
    semaphore = asyncio.Semaphore(max_concurrent)

    async with aiohttp.ClientSession() as session:
        tasks = [
            _geocode_single_opencage_async(
                session, semaphore, address, api_key, sleep_time, use_exponential_backoff
            )
            for address in addresses
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Convert results to DataFrame
    valid_results = [r for r in results if isinstance(r, dict)]
    result_df = pd.DataFrame(valid_results)

    # Merge with original dataframe
    output = dataframe.copy()
    for field in GEOCODING_FIELDS:
        output[field] = None

    if not result_df.empty:
        successes = 0
        failures = 0
        total_addresses = len(valid_results)

        for i, result in enumerate(valid_results):
            # Check if this result has valid coordinates
            has_coords = result.get("latitude") is not None and result.get("longitude") is not None
            if has_coords:
                successes += 1
            else:
                failures += 1

            for field in GEOCODING_FIELDS:
                if field in result:
                    output.at[i, field] = result[field]

            # Log progress every 100 addresses
            processed = i + 1
            _log_progress("OpenCage", processed, total_addresses, successes, failures)

        output["geocode"] = "opencage"

    log(f"Async OpenCage geocoding completed in {(time() - start_time): .2f} seconds")
    return output


async def geocode_geocodexyz_async(
    dataframe: pd.DataFrame,
    address_column: str,
    api_key: Optional[str] = None,
    max_concurrent: int = 5,
    sleep_time: float = 1.6,
    max_retries: int = MAX_RETRIES_DEFAULT,
    use_exponential_backoff: bool = True,
) -> pd.DataFrame:
    """Geocode addresses using Geocode.xyz API asynchronously."""
    log(f"Starting async geocoding of {len(dataframe)} addresses with Geocode.xyz")
    start_time = time()

    addresses = dataframe[address_column].tolist()
    semaphore = asyncio.Semaphore(max_concurrent)

    async with aiohttp.ClientSession() as session:
        tasks = [
            _geocode_single_geocodexyz_async(
                session,
                semaphore,
                address,
                api_key,
                sleep_time,
                use_exponential_backoff,
                max_retries,
            )
            for address in addresses
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Convert results to DataFrame
    valid_results = [r for r in results if isinstance(r, dict)]
    result_df = pd.DataFrame(valid_results)

    # Merge with original dataframe
    output = dataframe.copy()
    for field in GEOCODING_FIELDS:
        output[field] = None

    if not result_df.empty:
        successes = 0
        failures = 0
        total_addresses = len(valid_results)

        for i, result in enumerate(valid_results):
            # Check if this result has valid coordinates
            has_coords = result.get("latitude") is not None and result.get("longitude") is not None
            if has_coords:
                successes += 1
            else:
                failures += 1

            for field in GEOCODING_FIELDS:
                if field in result:
                    output.at[i, field] = result[field]

            # Log progress every 100 addresses
            processed = i + 1
            _log_progress("Geocode.xyz", processed, total_addresses, successes, failures)

        output["geocode"] = "geocode_xyz"

    successful_geocodes = output.dropna(subset=["latitude"]).shape[0]
    log(f"Found {successful_geocodes} addresses of {output.shape[0]} using async Geocode XYZ")
    log(f"Async Geocode.xyz geocoding completed in {(time() - start_time): .2f} seconds")
    return output


async def geocode_locationiq_async(
    dataframe: pd.DataFrame,
    address_column: str,
    api_key: str,
    max_concurrent: int = 10,
    sleep_time: float = DEFAULT_SLEEP_TIME,
    use_exponential_backoff: bool = True,
) -> pd.DataFrame:
    """Geocode addresses using LocationIQ API asynchronously."""
    log(f"Starting async geocoding of {len(dataframe)} addresses with LocationIQ")
    start_time = time()

    # Check API balance first
    try:
        async with aiohttp.ClientSession() as session:
            balance_url = f"https://us1.locationiq.com/v1/balance?key={api_key}&format=json"
            async with session.get(balance_url) as response:
                balance_data = await response.json()
                num_requests_left = int(balance_data.get("balance", {}).get("day", 0))
    except Exception as e:
        log(f"Erro ao verificar o saldo de requisições: {e}")
        return dataframe

    if num_requests_left == 0:
        log("LocationIQ rate limited")
        return dataframe

    # Limit addresses to available requests
    addresses = dataframe[address_column].iloc[:num_requests_left].tolist()
    semaphore = asyncio.Semaphore(max_concurrent)

    async with aiohttp.ClientSession() as session:
        tasks = [
            _geocode_single_locationiq_async(
                session, semaphore, address, api_key, sleep_time, use_exponential_backoff
            )
            for address in addresses
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Convert results to DataFrame
    valid_results = [r for r in results if isinstance(r, dict)]
    result_df = pd.DataFrame(valid_results)

    # Merge with original dataframe
    output = dataframe.copy()
    for field in GEOCODING_FIELDS:
        output[field] = None

    if not result_df.empty:
        successes = 0
        failures = 0
        total_addresses = len(valid_results)

        for i, result in enumerate(valid_results):
            if i < len(output):
                # Check if this result has valid coordinates
                has_coords = result.get("latitude") is not None and result.get("longitude") is not None
                if has_coords:
                    successes += 1
                else:
                    failures += 1

                for field in GEOCODING_FIELDS:
                    if field in result:
                        output.at[i, field] = result[field]

                # Log progress every 100 addresses
                processed = i + 1
                _log_progress("LocationIQ", processed, total_addresses, successes, failures)

        output["geocode"] = "locationiq"

    log(f"Async LocationIQ geocoding completed in {(time() - start_time): .2f} seconds")
    return output


# =============================================================================
# GEOAPIFY BATCH GEOCODING 
# =============================================================================

async def _submit_geoapify_batch_job(
    session: aiohttp.ClientSession,
    addresses: list,
    api_key: str,
) -> Optional[str]:
    """Submit a batch job to Geoapify and return job URL."""
    url = f"https://api.geoapify.com/v1/batch/geocode/search?lang=pt&filter=countrycode:br&apiKey={api_key}"

    # Geoapify Batch API expects a simple array of address strings
    batch_data = addresses

    try:
        async with session.post(
            url,
            json=batch_data,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as response:
            if response.status == 200:
                data = await response.json()
                job_url = data.get("url")
                log(f"Batch job submitted successfully: {job_url}")
                return job_url
            elif response.status == 402:
                log("Geoapify: API credits exhausted")
                return None
            elif response.status == 429:
                log("Geoapify: Rate limit exceeded")
                return None
            else:
                log(f"Geoapify batch submission failed: {response.status}")
                return None
    except Exception as e:
        log(f"Error submitting Geoapify batch job: {e}")
        return None


async def _check_geoapify_batch_status(
    session: aiohttp.ClientSession,
    job_url: str,
    api_key: str,
) -> Optional[Dict]:
    """Check the status of a Geoapify batch job."""
    try:
        async with session.get(
            f"{job_url}?apiKey={api_key}",
            timeout=aiohttp.ClientTimeout(total=15),
        ) as response:
            if response.status == 200:
                data = await response.json()
                return data
            else:
                log(f"Error checking Geoapify job status: {response.status}")
                return None
    except Exception as e:
        log(f"Error checking Geoapify batch status: {e}")
        return None


async def _process_geoapify_batch_results(
    results: list,
    addresses: list,
) -> list:
    """Process Geoapify batch results into standardized format."""
    processed_results = []
    updated_date = pd.Timestamp.now().strftime("%Y-%m-%d")

    # Geoapify batch returns results in the same order as the input addresses
    # Results array contains geocoding results for each address
    for i, address in enumerate(addresses):
        # Get the result for this address index
        result = results[i] if i < len(results) and results[i] else None

        if result and result.get("lat") is not None and result.get("lon") is not None:
            # Extract coordinates (Geoapify returns lat/lon directly in result)
            lat = result.get("lat")
            lon = result.get("lon")

            # Parse address components from Geoapify response
            street = result.get("street", "")
            housenumber = result.get("housenumber", "")
            # Use 'suburb' or 'district' for neighborhood
            district = result.get("suburb") or result.get("district", "")
            city = result.get("city", "")
            state = result.get("state", "")
            postcode = result.get("postcode", "")

            # Get confidence from rank object if available
            confidence = None
            if result.get("rank"):
                confidence = result["rank"].get("confidence")

            processed_result = {
                "address": address,
                "latitude": lat,
                "longitude": lon,
                "logradouro_geocode": unidecode(street).lower().strip() if street else None,
                "numero_porta_geocode": housenumber.strip() if housenumber else None,
                "bairro_geocode": unidecode(district).lower().strip() if district else None,
                "cidade_geocode": unidecode(city).lower().strip() if city else None,
                "estado_geocode": unidecode(state).lower().strip() if state else None,
                "cep_geocode": postcode.strip() if postcode else None,
                "confianca": confidence,
                "updated_date": updated_date,
                "geocode": "geoapify",
            }
        else:
            processed_result = _create_empty_async_result(address, updated_date, "geoapify")

        processed_results.append(processed_result)

    return processed_results


async def geocode_geoapify_batch_async(
    dataframe: pd.DataFrame,
    address_column: str,
    api_key: str,
    batch_size: int = 100,
    max_wait_time: int = 300,
    poll_interval: int = 5,
) -> pd.DataFrame:
    """Geocode addresses using Geoapify Batch API."""
    log(f"Starting batch geocoding of {len(dataframe)} addresses with Geoapify")
    start_time = time()

    addresses = dataframe[address_column].tolist()
    all_results = []

    # Process addresses in batches of specified size
    for batch_start in range(0, len(addresses), batch_size):
        batch_end = min(batch_start + batch_size, len(addresses))
        batch_addresses = addresses[batch_start:batch_end]
        batch_num = (batch_start // batch_size) + 1
        total_batches = (len(addresses) + batch_size - 1) // batch_size

        log(f"Processing batch {batch_num}/{total_batches} ({len(batch_addresses)} addresses)")

        async with aiohttp.ClientSession() as session:
            # Submit batch job
            job_url = await _submit_geoapify_batch_job(session, batch_addresses, api_key)

            if not job_url:
                # If submission failed, create empty results for this batch
                updated_date = pd.Timestamp.now().strftime("%Y-%m-%d")
                batch_results = [
                    _create_empty_async_result(addr, updated_date, "geoapify") for addr in batch_addresses
                ]
                all_results.extend(batch_results)
                continue

            # Poll for job completion
            waited_time = 0
            job_completed = False

            while waited_time < max_wait_time:
                await asyncio.sleep(poll_interval)
                waited_time += poll_interval

                status_data = await _check_geoapify_batch_status(session, job_url, api_key)

                if status_data and status_data.get("status") == "completed":
                    job_completed = True
                    batch_results = await _process_geoapify_batch_results(
                        status_data.get("results", []), batch_addresses
                    )
                    all_results.extend(batch_results)
                    log(f"Batch {batch_num} completed successfully")
                    break
                elif status_data and status_data.get("status") == "failed":
                    log(f"Batch {batch_num} failed")
                    break

            if not job_completed:
                log(f"Batch {batch_num} timed out after {max_wait_time} seconds")
                # Create empty results for this batch
                updated_date = pd.Timestamp.now().strftime("%Y-%m-%d")
                batch_results = [
                    _create_empty_async_result(addr, updated_date, "geoapify") for addr in batch_addresses
                ]
                all_results.extend(batch_results)

    # Convert results to DataFrame
    result_df = pd.DataFrame(all_results)

    # Merge with original dataframe
    output = dataframe.copy()
    for field in GEOCODING_FIELDS:
        output[field] = None

    if not result_df.empty:
        successes = 0
        failures = 0

        for i, result in enumerate(all_results):
            # Check if this result has valid coordinates
            has_coords = result.get("latitude") is not None and result.get("longitude") is not None
            if has_coords:
                successes += 1
            else:
                failures += 1

            for field in GEOCODING_FIELDS:
                if field in result:
                    output.at[i, field] = result[field]

        output["geocode"] = "geoapify"

        success_rate = (successes / len(all_results)) * 100 if all_results else 0
        log(f"Geoapify batch geocoding: {successes} sucessos, {failures} falhas ({success_rate:.1f}%)")

    successful_geocodes = output.dropna(subset=["latitude"]).shape[0]
    log(f"Found {successful_geocodes} addresses of {output.shape[0]} using Geoapify batch")
    log(f"Geoapify batch geocoding completed in {(time() - start_time): .2f} seconds")
    return output


# =============================================================================
# ASYNC WRAPPER FUNCTIONS
# =============================================================================


def run_geocode_nominatim_async(
    dataframe: pd.DataFrame,
    address_column: str,
    user_agent: str,
    domain: str,
    max_concurrent: int = 10,
    sleep_time: float = DEFAULT_SLEEP_TIME,
    use_exponential_backoff: bool = True,
    return_original_cols: bool = False,
) -> pd.DataFrame:
    """Wrapper to run async Nominatim geocoding in sync context."""
    try:
        return asyncio.run(
            geocode_nominatim_async(
                dataframe,
                address_column,
                user_agent,
                domain,
                max_concurrent,
                sleep_time,
                use_exponential_backoff,
                return_original_cols,
            )
        )
    except Exception as e:
        if "rate" in str(e).lower() or "429" in str(e) or "503" in str(e):
            log("Nominatim rate limited")
        else:
            log(f"Nominatim async geocoding failed: {e}")
        # Return empty result dataframe
        output = dataframe.copy()
        for field in GEOCODING_FIELDS:
            output[field] = None
        output["geocode"] = "nominatim"
        return output


def run_geocode_waze_async(
    dataframe: pd.DataFrame,
    address_column: str,
    max_concurrent: int = 10,
    sleep_time: float = DEFAULT_SLEEP_TIME,
    use_exponential_backoff: bool = True,
) -> pd.DataFrame:
    """Wrapper to run async Waze geocoding in sync context."""
    try:
        return asyncio.run(
            geocode_waze_async(
                dataframe,
                address_column,
                max_concurrent,
                sleep_time,
                use_exponential_backoff,
            )
        )
    except Exception as e:
        if "rate" in str(e).lower() or "limit" in str(e).lower():
            log("Waze rate limited")
        else:
            log(f"Waze async geocoding failed: {e}")
        # Return empty result dataframe
        output = dataframe.copy()
        for field in GEOCODING_FIELDS:
            output[field] = None
        output["geocode"] = "waze"
        return output


def run_geocode_maptiler_async(
    dataframe: pd.DataFrame,
    address_column: str,
    api_key: str,
    max_concurrent: int = 10,
    sleep_time: float = DEFAULT_SLEEP_TIME,
    use_exponential_backoff: bool = True,
) -> pd.DataFrame:
    """Wrapper to run async MapTiler geocoding in sync context."""
    try:
        return asyncio.run(
            geocode_maptiler_async(
                dataframe,
                address_column,
                api_key,
                max_concurrent,
                sleep_time,
                use_exponential_backoff,
            )
        )
    except Exception as e:
        if "rate" in str(e).lower() or "limit" in str(e).lower() or "quota" in str(e).lower():
            log("MapTiler rate limited")
        else:
            log(f"MapTiler async geocoding failed: {e}")
        # Return empty result dataframe
        output = dataframe.copy()
        for field in GEOCODING_FIELDS:
            output[field] = None
        output["geocode"] = "maptiler"
        return output


def run_geocode_opencage_async(
    dataframe: pd.DataFrame,
    address_column: str,
    api_key: str,
    max_concurrent: int = 10,
    sleep_time: float = DEFAULT_SLEEP_TIME,
    use_exponential_backoff: bool = True,
) -> pd.DataFrame:
    """Wrapper to run async OpenCage geocoding in sync context."""
    try:
        return asyncio.run(
            geocode_opencage_async(
                dataframe,
                address_column,
                api_key,
                max_concurrent,
                sleep_time,
                use_exponential_backoff,
            )
        )
    except Exception as e:
        if "402" in str(e) or "quota" in str(e).lower() or "limit" in str(e).lower():
            log("OpenCage rate limited")
        # Return empty result dataframe
        output = dataframe.copy()
        for field in GEOCODING_FIELDS:
            output[field] = None
        output["geocode"] = "opencage"
        return output


def run_geocode_geocodexyz_async(
    dataframe: pd.DataFrame,
    address_column: str,
    api_key: Optional[str] = None,
    max_concurrent: int = 5,
    sleep_time: float = 1.6,
    max_retries: int = MAX_RETRIES_DEFAULT,
    use_exponential_backoff: bool = True,
) -> pd.DataFrame:
    """Wrapper to run async Geocode.xyz geocoding in sync context."""
    try:
        return asyncio.run(
            geocode_geocodexyz_async(
                dataframe,
                address_column,
                api_key,
                max_concurrent,
                sleep_time,
                max_retries,
                use_exponential_backoff,
            )
        )
    except Exception as e:
        if "402" in str(e) or "quota" in str(e).lower() or "credits" in str(e).lower():
            log("Geocode.xyz rate limited")
        # Return empty result dataframe
        output = dataframe.copy()
        for field in GEOCODING_FIELDS:
            output[field] = None
        output["geocode"] = "geocode_xyz"
        return output


def run_geocode_locationiq_async(
    dataframe: pd.DataFrame,
    address_column: str,
    api_key: str,
    max_concurrent: int = 10,
    sleep_time: float = DEFAULT_SLEEP_TIME,
    use_exponential_backoff: bool = True,
) -> pd.DataFrame:
    """Wrapper to run async LocationIQ geocoding in sync context."""
    try:
        return asyncio.run(
            geocode_locationiq_async(
                dataframe,
                address_column,
                api_key,
                max_concurrent,
                sleep_time,
                use_exponential_backoff,
            )
        )
    except Exception as e:
        if "402" in str(e) or "quota" in str(e).lower() or "balance" in str(e).lower():
            log("LocationIQ rate limited")
        # Return empty result dataframe
        output = dataframe.copy()
        for field in GEOCODING_FIELDS:
            output[field] = None
        output["geocode"] = "locationiq"
        return output


def run_geoapify_batch_geocoding_async(
    dataframe: pd.DataFrame,
    address_column: str,
    api_key: str,
    batch_size: int = 100,
    max_wait_time: int = 300,
    poll_interval: int = 5,
) -> pd.DataFrame:
    """Wrapper to run async Geoapify batch geocoding in sync context."""
    try:
        return asyncio.run(
            geocode_geoapify_batch_async(
                dataframe,
                address_column,
                api_key,
                batch_size,
                max_wait_time,
                poll_interval,
            )
        )
    except Exception as e:
        if "rate" in str(e).lower() or "limit" in str(e).lower() or "quota" in str(e).lower():
            log("Geoapify rate limited")
        else:
            log(f"Geoapify batch geocoding failed: {e}")
        # Return empty result dataframe
        output = dataframe.copy()
        for field in GEOCODING_FIELDS:
            output[field] = None
        output["geocode"] = "geoapify"
        return output