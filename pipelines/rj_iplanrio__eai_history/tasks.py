# -*- coding: utf-8 -*-
import asyncio
from typing import Optional

import basedosdados as bd
from iplanrio.pipelines_utils.logging import log
from prefect import task

from pipelines.rj_iplanrio__eai_history import env
from pipelines.rj_iplanrio__eai_history.history import GoogleAgentEngineHistory


# A anotação @task deve estar na função que o Prefect irá chamar diretamente.
@task
def get_last_update(
    dataset_id: str,
    table_id: str,
    last_update: Optional[str] = None,
    enviroment: str = "staging",
) -> str:
    """
    Busca a data da última atualização da tabela no BigQuery.
    Esta é uma operação síncrona e bloqueante (I/O de rede/disco).
    """
    if last_update:
        log(f"'last_update' fornecido via parametro: {last_update}")
        return last_update

    if enviroment == "stagging":
        project_id = env.PROJECT_ID
        log(f"'enviroment' staging using project_id: {env.PROJECT_ID}")

    elif enviroment == "prod":
        project_id = env.PROJECT_ID_PROD
        log(f"'enviroment' prod using project_id: {env.PROJECT_ID_PROD}")
    else:
        raise (ValueError("enviroment must be prod or staging"))

    log(f"Buscando último 'last_update' para {dataset_id}.{table_id}")
    bd.config.billing_project_id = "rj-iplanrio"
    bd.config.from_file = True
    query = f"""
        SELECT
            MAX(last_update) as last_update
        FROM `rj-iplanrio.{dataset_id}_staging.{table_id}`
        WHERE project_id = '{project_id}'
    """
    log(msg=f"Runing query:\n{query}")

    result = bd.read_sql(query=query)

    if result is None or result.empty:
        log("Nenhum 'last_update' encontrado.")
        return "2025-07-25T00:00:00"

    last_update = result["last_update"][0]
    if last_update:
        log(f"Último 'last_update' encontrado: {last_update}")
        return last_update
    else:
        log("Nenhum 'last_update' encontrado.")
        return "2025-07-25T00:00:00"


@task
def fetch_history_data(
    last_update: str,
    session_timeout_seconds: Optional[int] = 3600,
    use_whatsapp_format: bool = True,
    max_user_save_limit: int = 100,
    enviroment: str = "staging",
) -> Optional[str]:
    """
    Ponto de entrada SÍNCRONO que orquestra a execução da lógica assíncrona,
    seguindo o padrão da `outra task`.
    """

    # --- Início da lógica assíncrona interna ---
    async def _main_async_runner() -> Optional[str]:
        log("Criando instância de GoogleAgentEngineHistory...")
        history_instance = await GoogleAgentEngineHistory.create(enviroment=enviroment)

        log(f"Buscando histórico a partir de: {last_update or 'início dos tempos'}.")
        data_path = await history_instance.get_history_bulk_from_last_update(
            last_update=last_update,
            session_timeout_seconds=session_timeout_seconds,
            use_whatsapp_format=use_whatsapp_format,
            max_user_save_limit=max_user_save_limit,
        )

        return data_path

    final_data_path = asyncio.run(_main_async_runner())
    return final_data_path
