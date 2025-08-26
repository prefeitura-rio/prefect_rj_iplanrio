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
    _build_geocode_xyz_url,
    _create_empty_geocoding_result,
    _is_valid_geocode_xyz_coordinates,
    _prepare_addresses_for_nominatim,
)


async def _async_exponential_backoff_sleep(
    attempt: int, base_time: float = BASE_BACKOFF_TIME
) -> None:
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


def _create_empty_async_result(
    address: str, updated_date: str, provider: str = "opencage"
) -> Dict[str, Any]:
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


def _log_progress(geocoder_name: str, processed: int, total: int, successes: int, 
                 failures: int, interval: int = 100) -> None:
    """Log progress for geocoding operations at specified intervals."""
    if processed % interval == 0 or processed == total:
        success_rate = (successes / processed * 100) if processed > 0 else 0
        log(f"[{geocoder_name}] Processados: {processed}/{total} endereços "
            f"({successes} sucessos, {failures} falhas) - {success_rate:.1f}% sucesso")


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
                    log(
                        f"Nominatim request failed with status {response.status} "
                        f"for address: {address}"
                    )
                    # Use utility function for empty result
                    empty_result = _create_empty_geocoding_result(updated_date)
                    geocode_result = dict(zip(GEOCODING_FIELDS, empty_result))
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
                    geocode_result = dict(zip(GEOCODING_FIELDS, empty_result))
                    geocode_result["address"] = address
                    geocode_result["geocode"] = "nominatim"

        except Exception:
            # Use utility function for empty result
            empty_result = _create_empty_geocoding_result(updated_date)
            geocode_result = dict(zip(GEOCODING_FIELDS, empty_result))
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
            "lang": "pt-BR"
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

        except Exception as e:
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
        search_address = f"{address}, Rio de Janeiro, Brazil" if "Rio de Janeiro" not in address else f"{address}, Brazil"
        
        url = f"https://api.maptiler.com/geocoding/{search_address}.json"
        params = {
            "key": api_key,
            "language": "pt",
            "limit": 1,
            "country": "br"
        }
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

        except Exception as e:
            geocode_result = _create_empty_async_result(address, updated_date, "maptiler")

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
            _geocode_single_waze_async(
                session, semaphore, address, sleep_time, use_exponential_backoff
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
            _geocode_single_maptiler_async(
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
            _log_progress("MapTiler", processed, total_addresses, successes, failures)
            
        output["geocode"] = "maptiler"

    successful_geocodes = output.dropna(subset=["latitude"]).shape[0]
    log(f"Found {successful_geocodes} addresses of {output.shape[0]} using async MapTiler")
    log(f"Async MapTiler geocoding completed in {(time() - start_time): .2f} seconds")
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