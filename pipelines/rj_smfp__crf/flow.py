"""Flow for rj_smfp__crf."""

from loguru import logger
from prefect import flow
from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from pipelines.rj_smfp__crf.tasks import get_max_date_from_bigquery_task, process_all_crf_zip_files_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from pipelines.rj_smfp__crf.env import CRF__BUCKET_NAME, CRF__FOLDER_PREFIX_PERIODOS_EVENTOS


@flow(log_prints=True)
def rj_smfp__crf(
    table_id: str,
    project_id: str = "rj-iplanrio",
    dataset_id: str = "brutos_crf",
    data_inicio: str | None = None,
    data_fim: str | None = None,
) -> None:
    """Processa uma tabela CRF do GCS e faz upload para o BigQuery.

    Executa o ciclo completo para uma das 4 tabelas CRF:
    1. Determinar o intervalo de datas a processar:
       - Se ``data_inicio`` e ``data_fim`` forem ambos ``None``, consulta
         automaticamente o BigQuery para obter a data máxima de partição,
         que é usada como ``data_inicio`` (marca d'água).
       - Caso contrário, usa os valores fornecidos para filtrar os ZIPs.
    2. Listar no GCS apenas os arquivos ZIP dentro do intervalo de datas.
    3. Para cada ZIP: baixar, extrair, ler FWF e salvar parquet localmente.
    4. Fazer upload dos parquets para o BigQuery em modo append.

    :param table_id: Identificador da tabela CRF a processar. Obrigatório.
        Valores válidos: ``'periodos_simples'``, ``'periodos_mei'``,
        ``'eventos_simples'``, ``'eventos_mei'``.
    :param project_id: Google Cloud project ID (padrão: ``'rj-iplanrio'``).
    :param dataset_id: ID do dataset BigQuery de destino (padrão: ``'brutos_crf'``).
    :param data_inicio: Data de início do intervalo no formato ``YYYY-MM-DD``.
        Arquivos com data igual ou anterior são ignorados. Se ``None`` junto
        com ``data_fim``, consulta automaticamente o BigQuery.
    :param data_fim: Data de fim do intervalo no formato ``YYYY-MM-DD``.
        Arquivos com data posterior são ignorados. Se ``None``, não aplica
        limite superior. Requer que ``data_inicio`` também seja fornecido.
    :raises ValueError: Se ``table_id`` não for um dos valores válidos.
    """
    valid_table_ids = ("periodos_simples", "periodos_mei", "eventos_simples", "eventos_mei")
    if table_id not in valid_table_ids:
        raise ValueError(
            f"table_id '{table_id}' inválido. "
            f"Válidos: {', '.join(repr(t) for t in valid_table_ids)}."
        )

    rename_current_flow_run_task(new_name=f"{project_id}.{dataset_id}.{table_id}")
    inject_bd_credentials_task(environment="prod")

    if data_inicio is None and data_fim is None:
        data_inicio = get_max_date_from_bigquery_task(
            project_id=project_id,
            dataset_id=f"{dataset_id}_staging",
            table_id=table_id,
        )

    data_path = process_all_crf_zip_files_task(
        project_id=project_id,
        bucket_name=CRF__BUCKET_NAME,
        folder_prefix=CRF__FOLDER_PREFIX_PERIODOS_EVENTOS,
        table_id=table_id,
        data_inicio=data_inicio,
        data_fim=data_fim,
    )

    if data_path is None:
        logger.info("Nenhum arquivo ZIP encontrado no intervalo especificado — flow encerrado sem upload.")
        return

    create_table_and_upload_to_gcs_task(
            data_path=data_path,
            dataset_id=dataset_id,
            dump_mode="append",
            source_format="parquet",
            table_id=table_id,
        )