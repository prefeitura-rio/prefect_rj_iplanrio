# -*- coding: utf-8 -*-
"""
Tasks for geolocation pipeline migrated to Prefect 3.x
"""

from pathlib import Path
from time import sleep, time
from uuid import uuid4

import pandas as pd
from basedosdados import Base
from google.cloud import bigquery
from iplanrio.pipelines_utils.env import getenv_or_action
from iplanrio.pipelines_utils.logging import log
from prefect import task

from pipelines.rj_crm__geolocalizacao_residencia.utils.async_utils import (
    run_geocode_maptiler_async,
    run_geocode_nominatim_async,
    run_geocode_waze_async,
)
from pipelines.rj_crm__geolocalizacao_residencia.utils.geo_utils import (
    GEOCODING_FIELDS,
    coordinates_to_pluscode,
    geocode_geocodexyz,
    geocode_locationiq,
    geocode_opencage,
)


@task
def download_data_from_bigquery(
    query: str, billing_project_id: str, bucket_name: str, limit: int = None
) -> pd.DataFrame:
    """Download batch of addresses from BigQuery for geocoding"""

    # Apply dynamic limit if provided
    if limit is not None:
        import re

        query = re.sub(r"LIMIT \d+", f"LIMIT {limit}", query)
        log(f"Querying batch of addresses from BigQuery (max {limit} records)")
    else:
        log("Querying batch of addresses from BigQuery")

    bq_client = bigquery.Client(
        credentials=Base(bucket_name=bucket_name)._load_credentials(mode="prod"),
        project=billing_project_id,
    )
    job = bq_client.query(query)
    while not job.done():
        sleep(1)
    log("Getting result from query")
    results = job.result()
    log("Converting result to pandas dataframe")
    dfr = results.to_dataframe()
    log(f"Downloaded {len(dfr)} addresses for geocoding batch")
    log("End download data from bigquery")

    return dfr


@task
def check_df_emptiness(dataframe: pd.DataFrame) -> bool:
    """
    Check if there are new addresses to georeference
    """
    return dataframe.empty


@task
def async_geocoding_dataframe(
    dataframe: pd.DataFrame,
    address_column: str = "address",
    max_concurrent_nominatim: int = 100,
    return_original_cols: bool = False,
) -> pd.DataFrame:
    """
    Async geocoding dataframe using only Nominatim.
    Processes all addresses in one batch with concurrent requests.

    Args:
        dataframe: DataFrame containing addresses to geocode
        address_column: Column name containing addresses
        max_concurrent_nominatim: Max concurrent requests for Nominatim
        return_original_cols: Whether to return original columns

    Returns:
        DataFrame with geocoded addresses
    """
    log("Start async georeferencing dataframe using Nominatim only")
    start_time = time()

    # Prepare dataframe
    dataframe = dataframe.dropna(subset=[address_column])
    dataframe = dataframe.reset_index(drop=True)
    total_addresses = len(dataframe)

    log(f"Processing {total_addresses} addresses in one batch")

    log("Geocoding with Nominatim")
    user_agent = getenv_or_action("NOMINATIM")
    domain = getenv_or_action("NOMINATIM_DOMAIN")

    final_dataframe = run_geocode_nominatim_async(
        dataframe=dataframe,
        address_column=address_column,
        user_agent=user_agent,
        domain=domain,
        max_concurrent=max_concurrent_nominatim,
        sleep_time=1.1,
        use_exponential_backoff=True,
        return_original_cols=return_original_cols,
    )

    # Only keep successfully geocoded addresses
    final_dataframe = final_dataframe.dropna(subset=["latitude", "longitude"])

    success_count = len(final_dataframe)
    log(f"Successfully geocoded {success_count}/{total_addresses} addresses")

    # Ensure string types for all columns, replacing None with empty string
    if not final_dataframe.empty:
        final_dataframe = final_dataframe.fillna("")
        final_dataframe[final_dataframe.columns] = final_dataframe[final_dataframe.columns].astype("str")
        log(f"Final result preview: \n{final_dataframe.iloc[0]}")

    total_time = time() - start_time
    final_success_rate = (len(final_dataframe) / total_addresses) * 100 if total_addresses > 0 else 0
    log(
        f"Async geocoding completed in {total_time: .2f}s - Final success "
        f"{len(final_dataframe)}/{total_addresses} ({final_success_rate: .1f}%)"
    )

    return final_dataframe


@task
def async_geocoding_dataframe_with_fallback(
    dataframe: pd.DataFrame,
    address_column: str = "address",
) -> pd.DataFrame:
    """
    Async geocoding dataframe with sequential fallback through multiple geocoders.
    Processes all addresses through geocoders sequentially while respecting rate limits.

    Args:
        dataframe: DataFrame containing addresses to geocode
        address_column: Column name containing addresses

    Returns:
        DataFrame with geocoded addresses from all successful geocoders
    """
    log("Start async georeferencing dataframe with sequential fallback")
    start_time = time()

    # Prepare dataframe
    dataframe = dataframe.dropna(subset=[address_column])
    dataframe = dataframe.reset_index(drop=True)
    total_addresses = len(dataframe)

    log(f"Processing {total_addresses} addresses with sequential fallback")

    # Track geocoder rate limit status
    geocoder_status = {
        "waze": {"rate_limited": False, "name": "Waze"},
        "geocode_xyz": {"rate_limited": False, "name": "Geocode.xyz"},
        "locationiq": {"rate_limited": False, "name": "LocationIQ"},
        "maptiler": {"rate_limited": False, "name": "MapTiler"},
        "opencage": {"rate_limited": False, "name": "OpenCage"},
    }

    # Initialize successful results and remaining failures
    successful_results = pd.DataFrame()
    remaining_failures = dataframe.copy()

    # Add empty latitude/longitude columns to remaining_failures if they don't exist
    if "latitude" not in remaining_failures.columns:
        remaining_failures[["latitude", "longitude"]] = None

    # Process addresses through geocoders sequentially
    geocoder_configs = [
        {
            "name": "waze",
            "display_name": "Waze",
            "function": run_geocode_waze_async,
            "secret_path": None,
            "secret_key": None,
            "params": {"max_concurrent": 3, "sleep_time": 3.0},
        },
        {
            "name": "maptiler",
            "display_name": "MapTiler",
            "function": run_geocode_maptiler_async,
            "secret_path": "/maptiler_token",
            "secret_key": "API_KEY",
            "params": {"max_concurrent": 10, "sleep_time": 1.1},
        },
        {
            "name": "geocode_xyz",
            "display_name": "Geocode.xyz",
            "function": geocode_geocodexyz,
            "secret_path": "/geocode_xyz",
            "secret_key": "API_KEY",
            "params": {"sleep_time": 1.6},
        },
        {
            "name": "locationiq",
            "display_name": "LocationIQ",
            "function": geocode_locationiq,
            "secret_path": "/locationiq",
            "secret_key": "API_KEY",
            "params": {"sleep_time": 1.1},
        },
        {
            "name": "opencage",
            "display_name": "OpenCage",
            "function": geocode_opencage,
            "secret_path": "/opencage",
            "secret_key": "API_KEY",
            "params": {"sleep_time": 1.1},
        },
    ]

    for step, config in enumerate(geocoder_configs, 1):
        if remaining_failures.empty:
            log("No more failed addresses to process")
            break

        if geocoder_status[config["name"]]["rate_limited"]:
            log(f"Skipping {config['display_name']} - previously rate limited")
            continue

        log(f"Step {step}: Proc. {len(remaining_failures)} addresses w. {config['display_name']}")

        try:
            # Get API key if required
            if config["secret_key"] and config["secret_path"]:
                log(f"Getting API key: {config['secret_key']} from {config['secret_path']}")
                api_key = getenv_or_action(config["secret_key"])
                log(f"PD FOR {config['display_name']}: {api_key}")

                # Process failed addresses with current geocoder (with API key)
                geocoder_result = config["function"](
                    dataframe=remaining_failures,
                    address_column=address_column,
                    api_key=api_key,
                    use_exponential_backoff=True,
                    **config["params"],
                )
            else:
                # Process failed addresses with geocoder that doesn't need API key
                geocoder_result = config["function"](
                    dataframe=remaining_failures,
                    address_column=address_column,
                    use_exponential_backoff=True,
                    **config["params"],
                )

            # Check for rate limiting by examining results
            all_empty = geocoder_result["latitude"].isna().all() and geocoder_result["longitude"].isna().all()

            if all_empty and len(remaining_failures) > 0:
                log(f"{config['display_name']} appears to be rate limited - skipping")
                geocoder_status[config["name"]]["rate_limited"] = True
                continue

            # Separate new successes and remaining failures
            new_successes = geocoder_result.dropna(subset=["latitude", "longitude"]).copy()
            remaining_failures = geocoder_result[
                geocoder_result["latitude"].isna() | geocoder_result["longitude"].isna()
            ].copy()

            # Add new successes to our successful results
            if not new_successes.empty:
                successful_results = pd.concat([successful_results, new_successes], ignore_index=True)

            geocoder_success = len(new_successes)
            log(f"{config['display_name']} geocoded {geocoder_success} additional addresses")

        except Exception as e:
            log(f"Error processing with {config['display_name']}: {e}")
            # Mark as rate limited if error suggests quota issues
            if any(term in str(e).lower() for term in ["402", "quota", "limit", "credits", "balance"]):
                geocoder_status[config["name"]]["rate_limited"] = True
            continue

    # Final results - ensure we only return rows with valid coordinates
    if not successful_results.empty:
        successful_results = successful_results.dropna(subset=["latitude", "longitude"])
        successful_results = successful_results.dropna(subset=["logradouro_geocode"])

    final_success_count = len(successful_results)

    # Ensure string types for all columns, replacing None with empty string
    if not successful_results.empty:
        successful_results = successful_results.fillna("")
        successful_results[successful_results.columns] = successful_results[successful_results.columns].astype("str")
        log(f"Final result preview: \n{successful_results.iloc[0]}")

    total_time = time() - start_time
    final_success_rate = (final_success_count / total_addresses) * 100 if total_addresses > 0 else 0
    log(
        f"Sequential fallback geocoding completed in {total_time: .2f}s - Final success "
        f"{final_success_count}/{total_addresses} ({final_success_rate: .1f}%)"
    )
    successful_results = successful_results[successful_results["logradouro_tratado"] != "nan"]
    successful_results = successful_results[successful_results["logradouro_geocode"].notna()]

    return successful_results


@task
def geoapify_batch_geocoding_task(
    dataframe: pd.DataFrame,
    address_column: str = "endereco_completo",
    batch_size: int = 100,
    return_original_cols: bool = True,
) -> pd.DataFrame:
    """
    Geocode addresses using Geoapify Batch API.

    Args:
        dataframe: DataFrame containing addresses to geocode
        address_column: Column name containing addresses
        batch_size: Number of addresses per batch (max 100 for Geoapify)
        return_original_cols: Whether to return original columns

    Returns:
        DataFrame with geocoded addresses
    """
    log("Starting Geoapify batch geocoding task")
    start_time = time()

    # Prepare dataframe
    dataframe = dataframe.dropna(subset=[address_column])
    dataframe = dataframe.reset_index(drop=True)
    total_addresses = len(dataframe)

    if total_addresses == 0:
        log("No addresses to geocode")
        return dataframe

    log(f"Processing {total_addresses} addresses with Geoapify batch API")

    # Get API key from secrets
    api_key = getenv_or_action("API_TOKEN")
    log(f"Token: {api_key[0:5]}")

    # For now, return empty results since we'd need to implement the full async Geoapify batch logic
    # This would require the full async batch implementation from the original code
    log("Geoapify batch geocoding not fully implemented - returning empty results")

    # Return empty result DataFrame with proper structure
    final_dataframe = dataframe.copy()
    for field in GEOCODING_FIELDS:
        final_dataframe[field] = None
    final_dataframe["geocode"] = "geoapify"

    total_time = time() - start_time
    log(f"Geoapify batch geocoding task completed in {total_time: .2f}s - No results (not implemented)")

    return pd.DataFrame()  # Return empty for now


@task
def dataframe_to_file(
    dataframe: pd.DataFrame, file_folder: str = "pipelines/data", file_format: str = "parquet"
) -> Path:
    """
    Save dataframe to file and return the folder path.

    Args:
        dataframe (pd.DataFrame): The dataframe to save
        file_folder (str): The folder where the file will be saved
        file_format (str): The format of the file (csv or parquet)

    Returns:
        Path: The folder path where the file was saved

    Raises:
        ValueError: If dataframe is empty or if file_folder is None
    """
    if dataframe.empty:
        raise ValueError("Cannot save empty dataframe")

    if not file_folder:
        raise ValueError("file_folder cannot be None or empty")

    file_folder_path = Path(file_folder)
    file_folder_path.mkdir(parents=True, exist_ok=True)
    file_path = file_folder_path / f"{uuid4()}.{file_format}"

    if file_format == "csv":
        dataframe.to_csv(file_path, index=False)
    elif file_format == "parquet":
        dataframe.to_parquet(file_path, index=False)

    log(f"\nDataframe saved on {file_folder_path}. Full path to file {file_path}\n")
    log(dataframe)

    return file_folder_path


@task
def add_plus_code_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a 'plus_code' column to the DataFrame using latitude and longitude.

    Args:
        df (pd.DataFrame): DataFrame with 'latitude' and 'longitude' columns.

    Returns:
        pd.DataFrame: DataFrame with an additional 'plus_code' column.

    Note:
        Unit testing is recommended to ensure correct plus code generation.
    """
    df = df.copy()
    df["pluscode"] = df.apply(lambda row: coordinates_to_pluscode(row["latitude"], row["longitude"]), axis=1)
    return df


@task
def merge_and_deduplicate_geocoding_results(dataframes: list) -> pd.DataFrame:
    """
    Merges multiple geocoding result dataframes and removes duplicates.

    Args:
        dataframes (list): List of pandas DataFrames with geocoding results

    Returns:
        pd.DataFrame: Combined dataframe with duplicates removed
    """
    log("Starting merge and deduplication of geocoding results")

    # Filter out empty dataframes
    valid_dataframes = [df for df in dataframes if not df.empty]

    if not valid_dataframes:
        log("No valid dataframes to merge")
        return pd.DataFrame()

    # Concatenate all dataframes
    combined_df = pd.concat(valid_dataframes, ignore_index=True)
    log(f"Combined {len(valid_dataframes)} dataframes into {len(combined_df)} total rows")

    # Remove duplicates based on address fields
    # Use logradouro_tratado, numero_porta, and bairro as the key for deduplication
    duplicate_cols = ["logradouro_tratado", "numero_porta", "bairro"]

    # Only deduplicate if these columns exist
    if all(col in combined_df.columns for col in duplicate_cols):
        original_count = len(combined_df)
        combined_df = combined_df.drop_duplicates(subset=duplicate_cols, keep="first")
        deduplicated_count = len(combined_df)
        removed_count = original_count - deduplicated_count
        log(f"rm {removed_count} duplicate addresses, keeping {deduplicated_count} unique adds")
    else:
        log("Deduplication columns not found, keeping all rows")

    # Ensure string types for all columns, replacing None with empty string
    if not combined_df.empty:
        combined_df = combined_df.fillna("")
        combined_df[combined_df.columns] = combined_df[combined_df.columns].astype("str")
        log(f"Final merged result preview: \n{combined_df.iloc[0]}")

    return combined_df
