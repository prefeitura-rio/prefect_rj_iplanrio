import asyncio
from pathlib import Path
from typing import Optional

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from prefect import task
from prefect.logging import get_logger

from pipelines.rj_iplanrio__eai_history.helpers import EaiSettings
from pipelines.rj_iplanrio__eai_history.history import GoogleAgentEngineHistory

logger = get_logger("toolkit")


# Epoch timestamp used as the default cursor when the BQ table is empty.
_DEFAULT_LAST_UPDATE = "1970-01-01T00:00:00+00:00"


# A anotação @task deve estar na função que o Prefect irá chamar diretamente.
@task
def get_last_update(
    dataset_id: str,
    table_id: str,
    last_update: Optional[str] = None,
    environment: str = "staging",
) -> str:
    """
    Busca o timestamp da última atualização da tabela no BigQuery.
    Esta é uma operação síncrona e bloqueante (I/O de rede/disco).
    """
    if last_update:
        logger.info(f"'last_update' fornecido via parametro: {last_update}")
        return last_update

    if environment == "staging":
        project_id = EaiSettings().EAI__PROJECT_ID
        logger.info(f"'environment' staging using project_id: {project_id}")

    elif environment == "prod":
        project_id = EaiSettings().EAI__PROJECT_ID_PROD
        logger.info(f"'environment' prod using project_id: {project_id}")
    else:
        raise (ValueError("environment must be prod or staging"))

    logger.info(f"Buscando último 'last_update' para {dataset_id}.{table_id}")
    client = bigquery.Client(project="rj-iplanrio")
    query = f"""
        SELECT
            max(last_update) AS last_update
        FROM `rj-iplanrio.{dataset_id}_staging.{table_id}`
        WHERE environment = '{environment}'
        AND last_update IS NOT NULL
        AND last_update != 'None'
    """
    logger.info(f"Runing query:\n{query}")

    try:
        client.get_table(f"rj-iplanrio.{dataset_id}_staging.{table_id}")
    except NotFound:
        logger.info(f"Tabela não existe `{project_id}.{dataset_id}.{table_id}`")
        return _DEFAULT_LAST_UPDATE

    result = list(client.query(query).result())

    if not result:
        logger.info("Nenhum 'last_update' encontrado.")
        return _DEFAULT_LAST_UPDATE

    last_update = result[0]["last_update"]
    if last_update:
        logger.info(f"Último 'last_update' encontrado: {last_update}")
        return str(last_update)
    else:
        logger.info("Nenhum 'last_update' encontrado.")
        return _DEFAULT_LAST_UPDATE


@task
def fetch_history_data(
    last_update: str,
    session_timeout_seconds: Optional[int] = 3600,
    use_whatsapp_format: bool = True,
    max_user_save_limit: int = 100,
    environment: str = "staging",
    sql_limit: Optional[int] = None,
) -> Optional[str]:
    """
    Ponto de entrada SÍNCRONO que orquestra a execução da lógica assíncrona,
    seguindo o padrão da `outra task`.
    """

    # --- Início da lógica assíncrona interna ---
    async def _main_async_runner() -> Optional[str]:
        logger.info("Criando instância de GoogleAgentEngineHistory...")
        history_instance = await GoogleAgentEngineHistory.create(
            environment=environment
        )

        logger.info(f"Buscando histórico a partir de 'last_update': {last_update}.")
        data_path = await history_instance.get_history_bulk_from_last_update(
            last_update=last_update,
            session_timeout_seconds=session_timeout_seconds,
            use_whatsapp_format=use_whatsapp_format,
            max_user_save_limit=max_user_save_limit,
            sql_limit=sql_limit,
        )

        return data_path

    final_data_path = asyncio.run(_main_async_runner())

    # check it the path has files
    if final_data_path:
        final_path = Path(final_data_path)
        files_fond = list(final_path.rglob("*"))
        if not files_fond:
            logger.warning(
                f"Nenhum arquivo encontrado em {final_data_path}. Retornando None.",
            )
            return None
        else:
            logger.info(
                f"Arquivos encontrados em {final_data_path}: {len(files_fond)}\n{files_fond[:10]}"
            )

    return final_data_path
