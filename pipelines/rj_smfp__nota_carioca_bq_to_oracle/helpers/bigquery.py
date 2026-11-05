"""Read the schema, export a consistent snapshot to GCS, clean the files."""

import time
from asyncio import run
from collections.abc import Callable
from datetime import UTC, datetime

from google.cloud import bigquery
from toolkit.gcs import create_store, delete_keys, list_sizes
from toolkit.telemetry import logger

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.dbt import skip_initial_quiet_wait
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.types import ExportedFile, SnapshotRequest, TableSchema, TableSnapshot


def list_tables(project: str, dataset_id: str) -> list[str]:
    """List the tables of a BigQuery dataset, without views, in alphabetical order."""
    client = bigquery.Client(project=project)
    tables = client.list_tables(f"{project}.{dataset_id}")
    return sorted(table.table_id for table in tables if table.table_type == "TABLE")


def get_table_schema(project: str, dataset_id: str, table_id: str) -> TableSchema:
    """Read the schema and the row count of a BigQuery table.

    Raise `ValueError` if the table has rows in the streaming buffer, which the extract
    does not export.
    """
    client = bigquery.Client(project=project)
    table = client.get_table(f"{project}.{dataset_id}.{table_id}")
    if table.streaming_buffer is not None:
        raise ValueError(f"{table_id} tem linhas no streaming buffer; a contagem não seria confiável.")
    fields = [{"name": field.name, "type": field.field_type, "mode": field.mode} for field in table.schema]
    logger.info("Schema de %s: %d colunas, %d linhas", table_id, len(fields), table.num_rows)
    return TableSchema(fields=fields, num_rows=table.num_rows or 0)


def get_last_modified(project: str, dataset_id: str, table_id: str) -> datetime:
    """Read the date of the last change of a BigQuery table, in UTC.

    Raise `ValueError` if BigQuery does not report it.
    """
    modified = bigquery.Client(project=project).get_table(f"{project}.{dataset_id}.{table_id}").modified
    if modified is None:
        raise ValueError(f"O BigQuery não informou a última alteração de {table_id}.")
    return modified


def extract_table_to_gcs(project: str, dataset_id: str, table_id: str, bucket: str, prefix: str) -> list[ExportedFile]:
    """Export a BigQuery table to GCS as gzip CSV, with no header.

    Return the files that were written, with their sizes, in the order of their names.
    """
    client = bigquery.Client(project=project)
    table = client.get_table(f"{project}.{dataset_id}.{table_id}")
    job_config = bigquery.ExtractJobConfig(
        destination_format=bigquery.DestinationFormat.CSV,
        compression=bigquery.Compression.GZIP,
        print_header=False,
    )
    job = client.extract_table(
        table, f"gs://{bucket}/{prefix}/part-*.csv.gz", job_config=job_config, location=table.location
    )
    job.result()
    sizes = list_sizes(create_store(bucket), f"{prefix}/")
    files = [ExportedFile(name=name, size=size) for name, size in sorted(sizes.items())]
    logger.info("Extract de %s gerou %d arquivos em gs://%s/%s", table_id, len(files), bucket, prefix)
    return files


def delete_blobs(bucket: str, blob_names: list[str]) -> None:
    """Delete the exported files from GCS, one object for each request."""
    run(delete_keys(create_store(bucket), blob_names))
    logger.info("Removidos %d arquivos de gs://%s", len(blob_names), bucket)


def quiet_wait_seconds(last_modified: list[datetime], now: datetime, quiet_seconds: float) -> float:
    """Return how long until the tables complete the period without changes.

    The result is zero if the period has passed.
    """
    if not last_modified:
        return 0.0
    return max(0.0, quiet_seconds - (now - max(last_modified)).total_seconds())


def changed_tables(before: dict[str, datetime], after: dict[str, datetime]) -> list[str]:
    """List the tables whose last change moved between two reads, alphabetically."""
    return sorted(table_id for table_id, modified in before.items() if after.get(table_id) != modified)


def export_consistent_snapshot(
    request: SnapshotRequest,
    report: Callable[[str], None],
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, TableSnapshot]:
    """Export the tables as one snapshot of BigQuery.

    dbt updates the tables one after the other. For all of them to come from the same
    run, the extract starts only after `quiet_seconds` without changes in any of them,
    and it runs again if one changes while it runs. The extract jobs cost nothing.
    `report` gets the progress messages, and `sleep` waits for a number of seconds.
    Return the exported tables by name. Raise `RuntimeError` if the tables do not settle
    within the limits.
    """
    waited = 0.0
    attempt = 0
    first_pass = True
    while attempt < request.max_attempts:
        modified = {
            table_id: get_last_modified(request.project, request.dataset_id, table_id) for table_id in request.table_ids
        }
        wait = quiet_wait_seconds(list(modified.values()), datetime.now(UTC), request.quiet_seconds)
        if wait > 0 and first_pass and skip_initial_quiet_wait(list(modified.values()), request.dbt_finished_at):
            report("Nenhuma tabela mudou depois do dbt desta execução; dispensando a espera sem alterações")
            wait = 0.0
        first_pass = False
        if wait > 0:
            if waited + wait > request.max_wait_seconds:
                raise RuntimeError(
                    f"O BigQuery não ficou {request.quiet_seconds:.0f}s sem alterações em "
                    + f"{request.max_wait_seconds:.0f}s de espera; nada foi carregado."
                )
            newest = max(modified.values())
            report(
                f"BigQuery alterado às {newest:%H:%M:%S} UTC; aguardando {wait:.0f}s sem alterações antes do extract"
            )
            sleep(wait)
            waited += wait
            continue

        attempt += 1
        schemas = {
            table_id: get_table_schema(request.project, request.dataset_id, table_id) for table_id in request.table_ids
        }
        snapshot = {
            table_id: TableSnapshot(
                table_id=table_id,
                schema=schemas[table_id],
                last_modified=modified[table_id],
                files=extract_table_to_gcs(
                    request.project,
                    request.dataset_id,
                    table_id,
                    request.bucket,
                    f"{request.prefix}/tentativa-{attempt}/{table_id}",
                ),
            )
            for table_id in request.table_ids
        }
        after = {
            table_id: get_last_modified(request.project, request.dataset_id, table_id) for table_id in request.table_ids
        }
        changed = changed_tables(modified, after)
        if not changed:
            return snapshot
        report(f"BigQuery mudou durante o extract em {changed}; descartando os arquivos e repetindo")
        delete_blobs(request.bucket, [exported.name for table in snapshot.values() for exported in table.files])
    raise RuntimeError(f"O BigQuery mudou durante {request.max_attempts} extracts seguidos; nada foi carregado.")
