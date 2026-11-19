"""Utilitários compartilhados entre os domínios CRF e DAF607."""

import shutil
from datetime import date
from pathlib import Path

from google.cloud import bigquery
from google.cloud.storage import Bucket, Client
from iplanrio.pipelines_utils.logging import log

from prefect_rj_iplanrio.sql import load_query

EXTRACT_BASE_PATH = "/tmp/rj_smfp__crf"


def get_max_date_from_bigquery(
    project_id: str,
    dataset_id: str,
    table_id: str,
) -> date | None:
    """Obtém a maior data de partição da tabela no BigQuery.

    Executa a query SQL para obter o valor máximo de ``data_particao``
    da tabela especificada, servindo como marca d'água para filtrar
    arquivos novos no bucket GCS.

    :param project_id: ID do projeto GCP.
    :param dataset_id: ID do dataset BigQuery.
    :param table_id: ID da tabela BigQuery.
    :returns: Data máxima como ``datetime.date`` ou ``None`` se a tabela estiver vazia.
    :raises Exception: Se houver erro ao executar a query.
    """
    try:
        query = load_query(
            __file__,
            "get_max_partition",
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
        )

        client = bigquery.Client(project=project_id)
        results = client.query(query).result()

        for row in results:
            max_date = row.max_data_particao
            if max_date:
                log(f"Watermark: {project_id}.{dataset_id}.{table_id} → {max_date}")
            else:
                log(f"Watermark: {project_id}.{dataset_id}.{table_id} → tabela vazia", level="warning")
            return max_date

    except Exception as e:
        log(f"Falha ao obter watermark de {dataset_id}.{table_id}: {e}", level="error")
        raise


def get_gcs_bucket(project_id: str, bucket_name: str) -> Bucket:
    """Cria e retorna um objeto Bucket do GCS autenticado.

    :param project_id: Google Cloud project ID.
    :param bucket_name: Nome do bucket.
    :returns: Objeto ``google.cloud.storage.Bucket``.
    """
    client = Client(project=project_id)
    return client.bucket(bucket_name)


def cleanup_extracted_directory(extract_path: str) -> None:
    """Remove recursivamente um diretório e todo o seu conteúdo.

    Deleta o diretório e todos os arquivos/subdiretórios contidos nele.
    Se o diretório não existir, retorna silenciosamente.

    :param extract_path: Caminho do diretório a ser removido.
    :raises Exception: Se houver erro ao remover arquivos.
    """
    extract_dir = Path(extract_path)

    if not extract_dir.exists():
        return

    try:
        shutil.rmtree(extract_dir)
    except Exception as e:
        raise Exception(f"Erro ao limpar diretório {extract_path}: {e}") from e
