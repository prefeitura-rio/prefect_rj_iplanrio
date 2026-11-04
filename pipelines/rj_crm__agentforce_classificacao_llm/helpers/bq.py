"""The native BigQuery tables of the pipeline: creation, reads, MERGE load.

The client carries the credentials of the environment. Without them the client falls
back to the default credentials, which in the pod are not the service account that has
access to `brutos_salesforce`.
"""

from collections.abc import Mapping, Sequence

from google.api_core.exceptions import Forbidden, NotFound
from google.cloud import bigquery
from pydantic import JsonValue
from toolkit.telemetry import logger

from pipelines.rj_crm__agentforce_classificacao_llm.helpers.schema import PRIMARY_KEY, SCHEMA
from utils.utils import load_credentials


def connect(project: str) -> bigquery.Client:
    """Return a BigQuery client of a project with the credentials of the environment."""
    return bigquery.Client(credentials=load_credentials(), project=project)


def read_rows(
    project: str, sql: str, parameters: Sequence[bigquery.ScalarQueryParameter] = ()
) -> list[dict[str, object]]:
    """Run a query and return its rows as dictionaries."""
    config = bigquery.QueryJobConfig(query_parameters=list(parameters))

    return [dict(row.items()) for row in connect(project).query(sql, job_config=config).result()]


def ensure_table(client: bigquery.Client, full_id: str, description: str, *, partitioned: bool) -> None:
    """Create a table only if it does not exist.

    Reading a table needs a permission that is common, and creating one does not. Asking
    first spares a 403 on every run for a table that is already there. Raise
    `PermissionError` if the table is missing and the service account cannot create it,
    so someone creates it by hand.
    """
    try:
        client.get_table(full_id)
        return
    except NotFound:
        pass

    table = bigquery.Table(full_id, schema=SCHEMA)
    table.description = description

    if partitioned:
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="data_particao"
        )
        table.clustering_fields = [PRIMARY_KEY]

    try:
        client.create_table(table, exists_ok=True)
    except Forbidden as error:
        raise PermissionError(
            f"The table {full_id} does not exist and the service account cannot create it. Create it by hand."
        ) from error


def ensure_tables(project: str, destination: str, tmp: str, descriptions: tuple[str, str]) -> None:
    """Check that the classification table and its tmp table exist.

    Create the one that is missing.
    """
    client = connect(project)
    ensure_table(client, destination, descriptions[0], partitioned=True)
    ensure_table(client, tmp, descriptions[1], partitioned=False)
    logger.info("Tables ready: %s and %s", destination, tmp)


def load_and_merge(
    project: str, tmp: str, rows: Sequence[Mapping[str, JsonValue]], merge_sql: str, truncate_sql: str
) -> int:
    """Add rows to the tmp table, MERGE them, clear it, and return the rows touched.

    Raise `RuntimeError` if the load has errors.
    """
    client = connect(project)
    config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND, schema=SCHEMA)
    job = client.load_table_from_json([dict(row) for row in rows], tmp, job_config=config)
    job.result()

    if job.errors:
        raise RuntimeError(f"The load of the tmp table has errors: {job.errors}")

    merge = client.query(merge_sql)
    merge.result()
    client.query(truncate_sql).result()

    return merge.num_dml_affected_rows or 0


def run_merge(project: str, sql: str) -> int:
    """Run a MERGE between tables that exist and return the rows that it touched."""
    job = connect(project).query(sql)
    job.result()

    return job.num_dml_affected_rows or 0
