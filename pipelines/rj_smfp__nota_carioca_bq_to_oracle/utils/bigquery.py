"""Leitura de schema, exportação consistente para o GCS e limpeza dos arquivos exportados."""

import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from google.cloud import bigquery, storage

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.dbt import skip_initial_quiet_wait
from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)

DELETE_BATCH_SIZE = 100
"""Máximo de objetos por requisição batch do GCS."""


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
    :returns: Dicionário com ``fields`` (lista de ``name``, ``type`` e ``mode``),
        ``num_rows`` e ``last_modified`` (última alteração, em UTC).
    :raises ValueError: Se a tabela tiver linhas no streaming buffer, que o
        extract não exporta.
    """
    client = bigquery.Client(project=project)
    table = client.get_table(f"{project}.{dataset_id}.{table_id}")
    if table.streaming_buffer is not None:
        raise ValueError(f"{table_id} tem linhas no streaming buffer; a contagem não seria confiável.")
    fields = [{"name": field.name, "type": field.field_type, "mode": field.mode} for field in table.schema]
    logger.info("Schema de %s: %d colunas, %d linhas", table_id, len(fields), table.num_rows)
    return {"fields": fields, "num_rows": table.num_rows, "last_modified": table.modified}


def get_last_modified(project: str, dataset_id: str, table_id: str) -> datetime:
    """Lê a data da última alteração de uma tabela do BigQuery.

    :param project: Projeto da tabela.
    :param dataset_id: Dataset da tabela.
    :param table_id: Nome da tabela.
    :returns: Última alteração, em UTC.
    :raises ValueError: Se o BigQuery não informar a última alteração.
    """
    modified = bigquery.Client(project=project).get_table(f"{project}.{dataset_id}.{table_id}").modified
    if modified is None:
        raise ValueError(f"O BigQuery não informou a última alteração de {table_id}.")
    return modified


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
    """Remove do GCS os arquivos exportados, em lotes de até 100 objetos por requisição.

    Cada lote é uma única requisição HTTP (batch do GCS), em vez de uma por objeto.

    :param project: Projeto usado pelo client do GCS.
    :param bucket: Bucket dos arquivos.
    :param blob_names: Nomes dos objetos a remover.
    :raises google.api_core.exceptions.GoogleAPICallError: Se algum objeto não puder ser removido,
        depois de tentar todos os do lote.
    """
    client = storage.Client(project=project)
    storage_bucket = client.bucket(bucket)
    for start in range(0, len(blob_names), DELETE_BATCH_SIZE):
        with client.batch(raise_exception=True):
            for name in blob_names[start : start + DELETE_BATCH_SIZE]:
                storage_bucket.blob(name).delete()
    logger.info("Removidos %d arquivos de gs://%s", len(blob_names), bucket)


@dataclass(frozen=True)
class SnapshotRequest:
    """Tabelas a exportar juntas, como uma foto única do BigQuery.

    :param project: Projeto das tabelas e dos jobs de extract.
    :param dataset_id: Dataset das tabelas.
    :param table_ids: Tabelas a exportar.
    :param bucket: Bucket de destino.
    :param prefix: Prefixo dos objetos no bucket, exclusivo do flow run.
    :param quiet_seconds: Tempo mínimo sem alterações em nenhuma das tabelas
        antes do extract.
    :param max_attempts: Extracts descartados, no máximo, por alteração durante
        a exportação.
    :param max_wait_seconds: Espera máxima, somada, por um período sem alterações.
    :param dbt_finished_at: Fim do run de dbt feito antes da carga, em UTC. Se nenhuma
        tabela mudou depois dele, a espera inicial é dispensada.
    """

    project: str
    dataset_id: str
    table_ids: list[str]
    bucket: str
    prefix: str
    quiet_seconds: float
    max_attempts: int = 5
    max_wait_seconds: float = 3600.0
    dbt_finished_at: datetime | None = None


@dataclass(frozen=True)
class TableSnapshot:
    """Tabela exportada na foto do BigQuery.

    :param table_id: Nome da tabela.
    :param schema: Schema e contagem de linhas, lidos na mesma foto do extract.
    :param last_modified: Última alteração da tabela na foto, igual antes e
        depois do extract.
    :param files: Arquivos exportados.
    """

    table_id: str
    schema: dict[str, object]
    last_modified: datetime
    files: list[ExportedFile]


def quiet_wait_seconds(last_modified: list[datetime], now: datetime, quiet_seconds: float) -> float:
    """Calcula quanto falta para as tabelas completarem o período sem alterações.

    :param last_modified: Última alteração de cada tabela.
    :param now: Horário atual.
    :param quiet_seconds: Período mínimo sem alterações.
    :returns: Segundos de espera; zero se o período já foi cumprido.
    """
    if not last_modified:
        return 0.0
    return max(0.0, quiet_seconds - (now - max(last_modified)).total_seconds())


def changed_tables(before: dict[str, datetime], after: dict[str, datetime]) -> list[str]:
    """Lista as tabelas alteradas entre duas leituras da última alteração.

    :param before: Última alteração de cada tabela antes do extract.
    :param after: Última alteração de cada tabela depois do extract.
    :returns: Tabelas cuja última alteração mudou, em ordem alfabética.
    """
    return sorted(table_id for table_id, modified in before.items() if after.get(table_id) != modified)


def export_consistent_snapshot(
    request: SnapshotRequest,
    report: Callable[[str], None],
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, TableSnapshot]:
    """Exporta as tabelas como uma foto única do BigQuery.

    O dbt atualiza as tabelas uma depois da outra. Para que todas venham do mesmo
    run, o extract só começa depois de ``quiet_seconds`` sem alterações em
    nenhuma delas, e é refeito se alguma mudar enquanto ele roda. Os jobs de
    extract não têm custo.

    :param request: Tabelas, destino e limites de espera.
    :param report: Função que recebe as mensagens de andamento.
    :param sleep: Função de espera, em segundos.
    :returns: Tabelas exportadas, pelo nome.
    :raises RuntimeError: Se as tabelas não ficarem estáveis dentro dos limites.
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
                    f"{request.max_wait_seconds:.0f}s de espera; nada foi carregado."
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
        delete_blobs(
            request.project,
            request.bucket,
            [exported.name for table in snapshot.values() for exported in table.files],
        )
    raise RuntimeError(f"O BigQuery mudou durante {request.max_attempts} extracts seguidos; nada foi carregado.")
