# -*- coding: utf-8 -*-
from google.cloud import bigquery
from basedosdados import Base
from time import sleep
import pandas as pd
import requests

from iplanrio.pipelines_utils.logging import log


def send_discord_webhook_message(
    webhook_url: str, message: str, timeout: int = 15
) -> dict:
    """
    Envia mensagem para o webhook retornando o payload da resposta (quando disponível).
    """
    if len(message) > 2000:
        raise ValueError(f"Mensagem excede limite de 2000 caracteres: {len(message)}.")

    params = {"wait": "true"}
    response = requests.post(
        webhook_url,
        json={"content": message},
        params=params,
        timeout=timeout,
    )
    if response.status_code not in (200, 204):
        raise ValueError(
            f"Falha ao enviar alerta ao Discord: {response.status_code} - {response.text}"
        )

    try:
        return response.json()
    except ValueError:
        return {}


def download_data_from_bigquery(
    query: str, billing_project_id: str, bucket_name: str
) -> pd.DataFrame:  # pandas
    """
    Execute a BigQuery SQL query and return results as a pandas DataFrame.

    Args:
        query (str): SQL query to execute in BigQuery
        billing_project_id (str): GCP project ID for billing purposes
        bucket_name (str): GCS bucket name for credential loading

    Returns:
        pd.DataFrame: Query results as a pandas DataFrame

    Raises:
        Exception: If BigQuery job fails or credentials cannot be loaded
    """
    log("Querying data from BigQuery")
    query = str(query)
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
    dfr = results.to_dataframe(create_bqstorage_client=False)
    log("End download data from bigquery")
    return dfr
