"""Tasks for GCS ZIP file handling and extraction."""

from datetime import date

from iplanrio.pipelines_utils.env import getenv_or_action
from prefect import task

from pipelines.rj_smfp__crf.utils import (
    get_max_date_from_bigquery,
    process_crf_zip_files,
)


@task(retries=3, retry_delay_seconds=10)
def get_max_date_from_bigquery_task(
    project_id: str,
    dataset_id: str,
    table_id: str,
) -> date | None:
    """Obtém a maior data de partição da tabela CRF no BigQuery.

    Executa a query SQL para obter o valor máximo de ``data_particao``
    da tabela especificada, servindo como marca d'água para filtrar
    arquivos ZIP novos no bucket GCS.

    :param project_id: ID do projeto GCP.
    :param dataset_id: ID do dataset BigQuery.
    :param table_id: ID da tabela BigQuery.
    :returns: Data máxima como ``datetime.date`` ou ``None`` se a tabela estiver vazia.
    :raises Exception: Se houver erro ao executar a query.
    """
    return get_max_date_from_bigquery(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
    )


@task
def process_all_crf_zip_files_task(
    project_id: str,
    table_id: str,
    data_inicio: date | str | None = None,
    data_fim: date | str | None = None,
) -> str | None:
    """Processa sequencialmente todos os arquivos ZIP CRF do GCS.

    Delega o ciclo completo (listar, baixar, extrair, ler FWF, salvar parquet,
    limpar) para :func:`~pipelines.rj_smfp__crf.utils.process_crf_zip_files`.

    O bucket e o prefixo da pasta vêm das variáveis de ambiente
    ``CRF__BUCKET_NAME`` e ``CRF__FOLDER_PREFIX_PERIODOS_EVENTOS``,
    lidas em runtime (injetadas pelo Infisical/container).

    :param project_id: Google Cloud project ID.
    :param table_id: Identificador da tabela CRF a processar.
    :param data_inicio: Data de início do intervalo (``YYYY-MM-DD`` ou ``date``).
        Arquivos com data igual ou anterior são ignorados. Se ``None``, sem limite inferior.
    :param data_fim: Data de fim do intervalo (``YYYY-MM-DD`` ou ``date``).
        Arquivos com data posterior são ignorados. Se ``None``, sem limite superior.
        Requer que ``data_inicio`` também seja fornecido.
    :returns: Caminho local (``data_path``) onde os arquivos parquet foram salvos,
        ou ``None`` se nenhum arquivo ZIP foi encontrado para processar.
    :raises ValueError: Se ``data_fim`` for fornecido sem ``data_inicio`` ou se
        alguma variável de ambiente obrigatória não estiver definida.
    """
    if data_fim is not None and data_inicio is None:
        raise ValueError(
            "data_fim não pode ser fornecido sem data_inicio. Forneça data_inicio para definir o início do intervalo."
        )

    return process_crf_zip_files(
        project_id=project_id,
        bucket_name=getenv_or_action("CRF__BUCKET_NAME"),
        folder_prefix=getenv_or_action("CRF__FOLDER_PREFIX_PERIODOS_EVENTOS"),
        table_id=table_id,
        data_inicio=data_inicio,
        data_fim=data_fim,
    )
