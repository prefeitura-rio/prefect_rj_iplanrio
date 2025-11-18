# -*- coding: utf-8 -*-
import asyncio
from typing import Optional
from pathlib import Path

import basedosdados as bd
from iplanrio.pipelines_utils.logging import log
from prefect import task

from pipelines.rj_iplanrio__eai_history import env
from pipelines.rj_iplanrio__eai_history.history import GoogleAgentEngineHistory


# A anotação @task deve estar na função que o Prefect irá chamar diretamente.
@task
def get_last_checkpoint_id(
    dataset_id: str,
    table_id: str,
    last_checkpoint_id: Optional[str] = None,
    environment: str = "staging",
) -> str:
    """
    Busca a data da última atualização da tabela no BigQuery.
    Esta é uma operação síncrona e bloqueante (I/O de rede/disco).
    """
    if last_checkpoint_id:
        log(f"'last_checkpoint_id' fornecido via parametro: {last_checkpoint_id}")
        return last_checkpoint_id

    if environment == "staging":
        project_id = env.PROJECT_ID
        log(f"'environment' staging using project_id: {project_id}")

    elif environment == "prod":
        project_id = env.PROJECT_ID_PROD
        log(f"'environment' prod using project_id: {project_id}")
    else:
        raise (ValueError("environment must be prod or staging"))

    log(f"Buscando último 'last_checkpoint_id' para {dataset_id}.{table_id}")
    bd.config.billing_project_id = "rj-iplanrio"
    bd.config.from_file = True
    query = f"""
        SELECT
            max(checkpoint_id) AS last_checkpoint_id,
        FROM `rj-iplanrio.{dataset_id}_staging.{table_id}`
        WHERE environment = '{environment}'
    """
    log(msg=f"Runing query:\n{query}")

    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
    tb_exists = tb.table_exists(mode="staging")

    if tb_exists:
        result = bd.read_sql(query=query)
    else:
        log(f"Tabela não existe `{project_id}.{dataset_id}.{table_id}`")
        return "0"

    if result is None or result.empty:
        log("Nenhum 'last_checkpoint_id' encontrado.")
        return "0"

    last_checkpoint_id = result["checkpoint_id"][0]
    if last_checkpoint_id:
        log(
            f"Últimos parametros encontrados 'last_checkpoint_id: {last_checkpoint_id}'"
        )
        return str(last_checkpoint_id)
    else:
        log("Nenhum 'last_checkpoint_id' encontrado.")
        return "0"


@task
def fetch_history_data(
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

        log(
            f"Buscando histórico a partir de 'last_checkpoint_id': {last_checkpoint_id}."
        )
        data_path = await history_instance.get_history_bulk_from_last_checkpoint_id(
            last_checkpoint_id=last_checkpoint_id,
            session_timeout_seconds=session_timeout_seconds,
            use_whatsapp_format=use_whatsapp_format,
            max_user_save_limit=max_user_save_limit,
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
        else:
            log(
                f"Arquivos encontrados em {final_data_path}: {len(files_fond)}\n{files_fond[:10]}"
            )

    return final_data_path
