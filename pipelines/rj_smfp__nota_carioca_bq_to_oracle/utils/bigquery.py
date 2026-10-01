"""Leitura de schema, exportação para o GCS e limpeza dos arquivos exportados."""

from dataclasses import dataclass

from google.cloud import bigquery, storage

from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class ExportedFile:
    """Arquivo CSV gzip gerado pelo extract no GCS.

    :param name: Nome do objeto no bucket.
    :param size: Tamanho do objeto em bytes (comprimido).
    """

    name: str
    size: int


def list_tables(project: str, dataset_id: str) -> list[str]:
    """Lista as tabelas (sem views) de um dataset do BigQuery.

    :param project: Projeto do dataset.
    :param dataset_id: Dataset de origem.
    :returns: Nomes das tabelas em ordem alfabética.
    """
    client = bigquery.Client(project=project)
    tables = client.list_tables(f"{project}.{dataset_id}")
    return sorted(table.table_id for table in tables if table.table_type == "TABLE")


def get_table_schema(project: str, dataset_id: str, table_id: str) -> dict[str, object]:
    """Lê o schema e a contagem de linhas de uma tabela do BigQuery.

    :param project: Projeto da tabela.
    :param dataset_id: Dataset da tabela.
    :param table_id: Nome da tabela.
    :returns: Dicionário com ``fields`` (lista de ``name``, ``type`` e ``mode``) e
        ``num_rows``.
    :raises ValueError: Se a tabela tiver linhas no streaming buffer, que o
        extract não exporta.
    """
    client = bigquery.Client(project=project)
    table = client.get_table(f"{project}.{dataset_id}.{table_id}")
    if table.streaming_buffer is not None:
        raise ValueError(f"{table_id} tem linhas no streaming buffer; a contagem não seria confiável.")
    fields = [{"name": field.name, "type": field.field_type, "mode": field.mode} for field in table.schema]
    logger.info("Schema de %s: %d colunas, %d linhas", table_id, len(fields), table.num_rows)
    return {"fields": fields, "num_rows": table.num_rows}


def extract_table_to_gcs(project: str, dataset_id: str, table_id: str, bucket: str, prefix: str) -> list[ExportedFile]:
    """Exporta uma tabela do BigQuery para o GCS como CSV gzip, sem cabeçalho.

    :param project: Projeto da tabela e do job de extract.
    :param dataset_id: Dataset da tabela.
    :param table_id: Nome da tabela.
    :param bucket: Bucket de destino.
    :param prefix: Prefixo dos objetos no bucket.
    :returns: Arquivos gerados, com tamanho, em ordem de nome.
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
    blobs = storage.Client(project=project).list_blobs(bucket, prefix=f"{prefix}/")
    files = sorted((ExportedFile(name=blob.name, size=int(blob.size or 0)) for blob in blobs), key=lambda f: f.name)
    logger.info("Extract de %s gerou %d arquivos em gs://%s/%s", table_id, len(files), bucket, prefix)
    return files


def delete_blobs(project: str, bucket: str, blob_names: list[str]) -> None:
    """Remove do GCS os arquivos exportados.

    :param project: Projeto usado pelo client do GCS.
    :param bucket: Bucket dos arquivos.
    :param blob_names: Nomes dos objetos a remover.
    """
    storage_bucket = storage.Client(project=project).bucket(bucket)
    for name in blob_names:
        storage_bucket.blob(name).delete()
    logger.info("Removidos %d arquivos de gs://%s", len(blob_names), bucket)
