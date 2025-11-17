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
    last_checkpoint_id: Optional[str] = None,
    environment: str = "staging",
) -> tuple[str, str]:
    """
    Busca a data da última atualização da tabela no BigQuery.
    Esta é uma operação síncrona e bloqueante (I/O de rede/disco).
    """
    if last_update:
        log(f"'last_update' fornecido via parametro: {last_update}")
        return last_update, last_checkpoint_id or "0"

    if environment == "staging":
        project_id = env.PROJECT_ID
        log(f"'environment' staging using project_id: {project_id}")

    elif environment == "prod":
        project_id = env.PROJECT_ID_PROD
        log(f"'environment' prod using project_id: {project_id}")
    else:
        raise (ValueError("environment must be prod or staging"))

    log(f"Buscando último 'last_update' para {dataset_id}.{table_id}")
    bd.config.billing_project_id = "rj-iplanrio"
    bd.config.from_file = True
    query = f"""
        WITH tb AS (
            SELECT
                max(chekpoint_id) AS last_checkpoint_id,
            FROM `rj-iplanrio.{dataset_id}_staging.{table_id}`
            WHERE environment = '{environment}'
            )

        SELECT checkpoint_id, last_update
        FROM `rj-iplanrio.{dataset_id}_staging.{table_id}` AS main
        WHERE environment = '{environment}' AND checkpoint_id = (SELECT last_checkpoint_id FROM tb)
    """
    log(msg=f"Runing query:\n{query}")

    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
    tb_exists = tb.table_exists(mode="staging")

    if tb_exists:
        result = bd.read_sql(query=query)
    else:
        log(f"Tabela não existe `{project_id}.{dataset_id}.{table_id}`")
        return "2025-07-25T00:00:00", "0"

    if result is None or result.empty:
        log("Nenhum 'last_update' encontrado.")
        return "2025-07-25T00:00:00", "0"

    last_update = result["last_update"][0]
    last_checkpoint_id = result["checkpoint_id"][0]
    if last_update:
        log(
            f"Últimos parametros encontrados\n'last_update: {last_update}\n'last_checkpoint_id: {last_checkpoint_id}'"
        )
        return last_update, str(last_checkpoint_id)
    else:
        log("Nenhum 'last_update' encontrado.")
        return "2025-07-25T00:00:00", "0"


@task
def fetch_history_data(
    last_update: str,
    last_checkpoint_id: str,
    session_timeout_seconds: Optional[int] = 3600,
    use_whatsapp_format: bool = True,
    max_user_save_limit: int = 100,
    environment: str = "staging",
) -> Optional[str]:
    """
    Ponto de entrada SÍNCRONO que orquestra a execução da lógica assíncrona,
    seguindo o padrão da `outra task`.
    """

    # --- Início da lógica assíncrona interna ---
    async def _main_async_runner() -> Optional[str]:
        log("Criando instância de GoogleAgentEngineHistory...")
        history_instance = await GoogleAgentEngineHistory.create(
            environment=environment
        )

        log(f"Buscando histórico a partir de: {last_update or 'início dos tempos'}.")
        data_path = await history_instance.get_history_bulk_from_last_update(
            last_update=last_update,
            last_checkpoint_id=last_checkpoint_id,
            session_timeout_seconds=session_timeout_seconds,
            use_whatsapp_format=use_whatsapp_format,
            max_user_save_limit=max_user_save_limit,
        )

        return data_path

    final_data_path = asyncio.run(_main_async_runner())
    return final_data_path
