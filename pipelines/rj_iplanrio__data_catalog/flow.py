# -*- coding: utf-8 -*-
import subprocess
from json import dumps, loads
from os import environ
from pathlib import Path

import polars as pl
from google.cloud import bigquery, storage
from prefect import flow


def get_catalog():
    """Extract the data catalog from BigQuery and save it as a JSON file."""
    client = bigquery.Client()
    query = Path("bigquery.sql").read_text()
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
            "transform.jq",
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
    client = storage.Client()
    bucket = client.bucket(environ["DATA_CATALOG__BUCKET_NAME"])
    blob = bucket.blob("catalog.json")
    blob.upload_from_filename("/tmp/catalog.json")


@flow(log_prints=True)
def rj_iplanrio__dados_mestres():
    get_catalog()
    transform_catalog()
    upload_catalog_to_bucket()
