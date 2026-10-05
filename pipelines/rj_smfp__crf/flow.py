"""Flow for rj_smfp__crf."""

from prefect import flow
from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from pipelines.rj_smfp__crf.tasks import get_max_date_from_bigquery_task, process_all_crf_zip_files_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from pipelines.rj_smfp__crf.env import CRF__BUCKET_NAME, CRF__FOLDER_PREFIX_PERIODOS_EVENTOS


@flow(log_prints=True)
def rj_smfp__crf(
    table_id: str,
    project_id: str = "rj-rec-rio",
    dataset_id: str = "brutos_crf",
    max_date_bigquery: str | None = None,
) -> None:
    """Processa uma tabela CRF do GCS e faz upload para o BigQuery.

    Executa o ciclo completo para uma das 4 tabelas CRF:
    1. Consultar a data máxima de partição no BigQuery (ou usar ``max_date_bigquery``).
    2. Listar no GCS apenas os arquivos ZIP com data posterior à marca d'água.
    3. Para cada ZIP: baixar, extrair, ler FWF e salvar parquet localmente.
    4. Fazer upload dos parquets para o BigQuery em modo append.

    :param table_id: Identificador da tabela CRF a processar. Obrigatório.
        Valores válidos: ``'periodos_simples'``, ``'periodos_mei'``,
        ``'eventos_simples'``, ``'eventos_mei'``.
    :param project_id: Google Cloud project ID (padrão: ``'rj-rec-rio'``).
    :param dataset_id: ID do dataset BigQuery de destino (padrão: ``'brutos_crf'``).
    :param max_date_bigquery: Data máxima de partição no formato ``YYYY-MM-DD``
        usada como marca d'água. Se ``None``, consulta automaticamente o BigQuery.
    :raises ValueError: Se ``table_id`` não for um dos valores válidos.
    """
    valid_table_ids = ("periodos_simples", "periodos_mei", "eventos_simples", "eventos_mei")
    if table_id not in valid_table_ids:
        raise ValueError(
            f"table_id '{table_id}' inválido. "
            f"Válidos: {', '.join(repr(t) for t in valid_table_ids)}."
        )

    rename_current_flow_run_task(new_name=f"{project_id}.{dataset_id}.{table_id}")
    # inject_bd_credentials_task(environment="prod")

    if max_date_bigquery is None:
        max_date = get_max_date_from_bigquery_task(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
        )
    else:
        max_date = max_date_bigquery

    data_path = process_all_crf_zip_files_task(
        project_id=project_id,
        bucket_name=CRF__BUCKET_NAME,
        folder_prefix=CRF__FOLDER_PREFIX_PERIODOS_EVENTOS,
        table_id=table_id,
        max_date_from_bq=max_date,
    )

    create_table_and_upload_to_gcs_task(
            data_path=data_path,
            dataset_id=dataset_id,
            dump_mode="append",
            source_format="parquet",
            table_id=table_id,
        )