import io
import zipfile
from os import read
from pathlib import Path

import laspy
import numpy as np
import polars as pl
from google.cloud import storage
from iplanrio.pipelines_utils.logging import log
from prefect import task
import glob

@task
def download_zip_from_gcs_task(
    bucket_name: str,
    blob_name: str,
    extract_path: str,
    project_id: str | None = None,
) -> str:
    """Download a ZIP file from GCS and extract its contents locally.

    Downloads the ZIP blob from Google Cloud Storage to memory, extracts all
    files to the specified directory, and returns the extraction directory path.

    :param bucket_name: Name of the GCS bucket.
    :param blob_name: Full blob name/path in the bucket (e.g., 'data/mit/file.zip').
    :param extract_path: Local directory path where files will be extracted.
    :param project_id: GCP project ID. If None, uses the default from the environment.
    :returns: Path to the extraction directory.
    :raises FileNotFoundError: If the blob does not exist in the bucket.
    :raises zipfile.BadZipFile: If the downloaded file is not a valid ZIP.
    """
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    if not blob.exists():
        raise FileNotFoundError(f"Blob '{blob_name}' not found in bucket '{bucket_name}'")

    log(f"Downloading '{blob_name}' from bucket '{bucket_name}'...")
    zip_content = blob.download_as_bytes()

    extract_dir = Path(extract_path)
    extract_dir.mkdir(parents=True, exist_ok=True)

    try:
        with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_file:
            zip_file.extractall(extract_dir)
    except zipfile.BadZipFile as e:
        raise zipfile.BadZipFile(f"File '{blob_name}' is not a valid ZIP file") from e

    log(f"Extracted to '{extract_dir}'")
    return str(extract_dir)

def read_las_file(file_path: str) -> pl.DataFrame:
    """Read a LAS file and return its contents as a Polars DataFrame.

    :param file_path: Path to the LAS file.
    :returns: Polars DataFrame containing the point cloud data.
    :raises FileNotFoundError: If the LAS file does not exist.
    :raises laspy.errors.LaspyException: If there is an error reading the LAS file.
    """
    if not Path(file_path).is_file():
        raise FileNotFoundError(f"LAS file '{file_path}' does not exist")

    try:
        las_data = laspy.read(file_path)
    except Exception as e:
        raise laspy.errors.LaspyException(f"Error reading LAS file '{file_path}'") from e

    return pl.DataFrame({dimension: np.array(las_data[dimension]) for dimension in las_data.point_format.dimension_names})  # noqa: E501

def read_csv(file_path: str) -> pl.DataFrame:
    """Read a CSV file and return its contents as a Polars DataFrame.

    :param file_path: Path to the CSV file.
    :returns: Polars DataFrame representing the CSV data.
    :raises FileNotFoundError: If the CSV file does not exist.
    :raises ValueError: If there is an error parsing the CSV file.
    """
    return pl.read_csv(file_path)

def read_json(file_path: str) -> pl.DataFrame:
    """Read a JSON file and return its contents as a Polars DataFrame.

    :param file_path: Path to the JSON file.
    :returns: Polars DataFrame representing the JSON data.
    :raises FileNotFoundError: If the JSON file does not exist.
    :raises ValueError: If there is an error parsing the JSON file.
    """
    return pl.read_json(file_path)

def read_file_files(file_path: str, table_id: str, format: str) -> list[str]:
    """Extract a ZIP file and return the list of extracted file paths.

    :param file_path: Path to the ZIP file.
    :param table_id: Identifier of the table/dataset. Must be one of:
        'lidar_mare', 'lidar_vidigal', 'edge_analysis', 'matched'.
    :param format: File format (unused, reserved for future use).
    :returns: List of absolute paths of the extracted files.
    :raises ValueError: If table_id is not supported.
    :raises FileNotFoundError: If the ZIP file does not exist.
    :raises zipfile.BadZipFile: If the file is not a valid ZIP.
    """
    if table_id not in ("lidar_mare", "lidar_vidigal", "edge_analysis", "matched"):
        raise ValueError(f"Unsupported table_id: {table_id}")

    zip_path = Path(file_path)
    if not zip_path.is_file():
        raise FileNotFoundError(f"ZIP file '{file_path}' does not exist")

    extract_dir = zip_path.parent
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(extract_dir)
        extracted = [str(extract_dir / name) for name in zf.namelist()]

    return extracted