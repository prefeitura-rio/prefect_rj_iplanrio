"""The BigQuery reads of the pipeline.

They use query parameters, the safe way to pass the name of an HSM.
"""

from datetime import date

from google.cloud import bigquery
from pydantic import TypeAdapter

from pipelines.rj_crm__relatorio_engajamento_hsm.constants import PROJECT_ID, SOURCES
from pipelines.rj_crm__relatorio_engajamento_hsm.types import Dispatch, Message
from utils.utils import load_credentials

MESSAGES: TypeAdapter[list[Message]] = TypeAdapter(list[Message])


def connect() -> bigquery.Client:
    """Return a client with the credentials of the environment, which carry the Drive scope too.

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


def read_messages(sql: str, hsm_name: str, start: date) -> list[Message]:
    """Return the messages of the sessions that got an HSM since a day."""
    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("nome_hsm", "STRING", hsm_name),
            bigquery.ScalarQueryParameter("data_inicio", "DATE", start),
            bigquery.ArrayQueryParameter("fontes", "STRING", list(SOURCES)),
        ]
    )
    rows = connect().query(sql, job_config=config).result()

    return MESSAGES.validate_python([dict(row) for row in rows])
