# -*- coding: utf-8 -*-
import asyncio
from typing import Optional
from pathlib import Path

import basedosdados as bd
from iplanrio.pipelines_utils.logging import log
from prefect import task

from pipelines.rj_iplanrio__eai_history import env
from pipelines.rj_iplanrio__eai_history.history import GoogleAgentEngineHistory


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
        log(f"'last_update' fornecido via parametro: {last_update}")
        return last_update

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
        SELECT
            max(last_update) AS last_update
        FROM `rj-iplanrio.{dataset_id}_staging.{table_id}`
        WHERE environment = '{environment}'
        AND last_update IS NOT NULL
        AND last_update != 'None'
    """
    log(msg=f"Runing query:\n{query}")

    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
    tb_exists = tb.table_exists(mode="staging")

    if tb_exists:
        result = bd.read_sql(query=query)
    else:
        log(f"Tabela não existe `{project_id}.{dataset_id}.{table_id}`")
        return _DEFAULT_LAST_UPDATE

    if result is None or result.empty:
        log("Nenhum 'last_update' encontrado.")
        return _DEFAULT_LAST_UPDATE

    last_update = result["last_update"][0]
    if last_update:
        log(f"Último 'last_update' encontrado: {last_update}")
        return str(last_update)
    else:
        log("Nenhum 'last_update' encontrado.")
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
        log("Criando instância de GoogleAgentEngineHistory...")
        history_instance = await GoogleAgentEngineHistory.create(
            environment=environment
        )

        log(f"Buscando histórico a partir de 'last_update': {last_update}.")
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
            log(
                f"Nenhum arquivo encontrado em {final_data_path}. Retornando None.",
                level="warning",
            )
            return None
        else:
            log(
                f"Arquivos encontrados em {final_data_path}: {len(files_fond)}\n{files_fond[:10]}"
            )

    return final_data_path
