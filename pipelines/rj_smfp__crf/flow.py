"""Flow for rj_smfp__crf."""

from prefect import flow
from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from pipelines.rj_smfp__crf.tasks import get_max_date_from_bigquery_task, process_all_crf_zip_files_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from pipelines.rj_smfp__crf.env import CRF__BUCKET_NAME, CRF__FOLDER_PREFIX_PERIODOS_EVENTOS, CRF__PROJECT_ID


@flow(log_prints=True)
def rj_smfp__crf(
    table_id: str | None = None,
    project_id: str = CRF__PROJECT_ID,
    dataset_id: str = "smfp_crf",
    extract_base_path: str = "/tmp/rj_smfp__crf",
    max_date_bigquery: str | None = None,
) -> None:
    """Processa uma tabela CRF do GCS.

    1. Obter data máxima de partição (via BigQuery ou parâmetro ``max_date_bigquery``).
    2. Listar arquivos ZIP no GCS com data maior que a partição máxima.
    3. Para cada arquivo ZIP: baixar, descompactar, ler FWF e limpar.

    :param table_id: Identificador da tabela CRF a processar
        (``periodo_simples``, ``periodos_mei``, ``eventos_simples``, ``eventos_mei``).
    :param project_id: Google Cloud project ID.
    :param dataset_id: ID do dataset BigQuery onde a tabela CRF está.
    :param extract_base_path: Diretório base local para arquivos descompactados.
    :param max_date_bigquery: Data máxima de partição no formato string (``YYYY-MM-DD``).
        Se ``None``, consulta automaticamente a data máxima do BigQuery.
    """
    rename_current_flow_run_task(new_name=f"{project_id}.{dataset_id}.{table_id}")
    inject_bd_credentials_task(environment="prod")

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
        extract_base_path=extract_base_path,
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