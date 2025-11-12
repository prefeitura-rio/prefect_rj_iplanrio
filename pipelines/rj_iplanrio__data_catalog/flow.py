import subprocess
from json import dumps, loads
from os import environ
from pathlib import Path

import polars as pl
from google.cloud import bigquery, storage
from google.oauth2 import service_account
from prefect import flow


def pull_secret_to_file() -> None:
    """Pull the service account JSON from an environment variable and save it to a file."""
    Path("/tmp/sa.json").write_text(environ["DATA_CATALOG__SERVICE_ACCOUNT"])


def get_credentials() -> service_account.Credentials:
    """Get Google Cloud credentials from the service account file."""
    return service_account.Credentials.from_service_account_file("/tmp/sa.json")


def get_catalog():
    """Query BigQuery to get the data catalog and save it as a raw JSON file."""
    credentials = get_credentials()
    client = bigquery.Client(credentials=credentials)
    query = (Path(__file__).parent / "bigquery.sql").read_text()
    result = client.query_and_wait(query).to_arrow()

    df = pl.from_arrow(result)
    df = df.filter(
        ~pl.col("dataset").str.contains("staging"),
        ~pl.col("dataset").str.contains("airbyte"),
        ~pl.col("dataset").str.contains("logs"),
    )

    df.write_json(Path("/tmp/catalog-raw.json"))


def transform_catalog():
    """Transform the raw catalog JSON using jq."""
    result = subprocess.run(
        [
            "jq",
            "-f",
            (Path(__file__).parent / "transform.jq").as_posix(),
            "/tmp/catalog-raw.json",
        ],
        capture_output=True,
        text=True,
        check=True,
    )

    transformed_json = loads(result.stdout)

    Path("/tmp/catalog.json").write_text(dumps(result.stdout))


def upload_catalog_to_bucket():
    """Upload the transformed catalog JSON to a Google Cloud Storage bucket."""
    credentials = get_credentials()
    client = storage.Client(credentials=credentials)
    bucket = client.bucket(environ["DATA_CATALOG__BUCKET_NAME"])
    blob = bucket.blob("catalog.json")
    blob.upload_from_filename("/tmp/catalog.json")


@flow(log_prints=True)
def rj_iplanrio__data_catalog():
    pull_secret_to_file()
    get_catalog()
    transform_catalog()
    upload_catalog_to_bucket()
