"""The BigQuery reads of the pipeline.

They use query parameters, the safe way to pass the name of an HSM.
"""

from datetime import date

import polars as pl
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from pydantic import TypeAdapter

from pipelines.rj_crm__relatorio_engajamento_hsm.constants import EXAMPLES_SCHEMA, PROJECT_ID, SOURCES
from pipelines.rj_crm__relatorio_engajamento_hsm.types import Dispatch, Message, Window
from utils.utils import load_credentials

MESSAGES: TypeAdapter[list[Message]] = TypeAdapter(list[Message])


def connect() -> bigquery.Client:
    """Return a client with the environment credentials, with the Drive scope too.

    The table of dispatches is a Google Sheet imported into BigQuery, so reading it
    needs that scope.
    """
    return bigquery.Client(credentials=load_credentials(), project=PROJECT_ID)


def read_dispatches(sql: str, reference: date) -> list[Dispatch]:
    """Return the dispatches that asked for a report on a day."""
    config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("data_referencia", "DATE", reference)]
    )
    rows = connect().query(sql, job_config=config).result()

    return [Dispatch(str(row.nome_campanha), row.relatorio_engajamento_data_disparo) for row in rows]


def read_messages(sql: str, window: Window) -> list[Message]:
    """Return the messages of the sessions that got an HSM or a campaign in a window."""
    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("value", "STRING", window.name),
            bigquery.ScalarQueryParameter("start", "DATE", window.start),
            bigquery.ScalarQueryParameter("end", "DATE", window.end),
            bigquery.ArrayQueryParameter("sources", "STRING", list(SOURCES)),
        ]
    )
    rows = connect().query(sql, job_config=config).result()

    return MESSAGES.validate_python([dict(row) for row in rows])


def read_examples(sql: str) -> pl.DataFrame:
    """Return the table of reviewed examples, or an empty one if it does not exist yet.

    The first run of the pipeline finds no table, and that is not an error.
    """
    try:
        rows = connect().query(sql).result()
    except NotFound:
        return pl.DataFrame(schema=EXAMPLES_SCHEMA)

    return pl.DataFrame([dict(row) for row in rows], schema=EXAMPLES_SCHEMA)
