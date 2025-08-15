import basedosdados as bd
from iplanrio.pipelines_utils.logging import log
from typing import Optional

from pipelines.rj_iplanrio__eai_history.db import GoogleAgentEngineHistory
from prefect import task
from prefect.utilities.asyncutils import run_sync_in_worker_thread
import asyncio


# A anotação @task deve estar na função que o Prefect irá chamar diretamente.
@task
def get_last_update(dataset_id: str, table_id: str) -> Optional[str]:
    """
    Busca a data da última atualização da tabela no BigQuery.
    Esta é uma operação síncrona e bloqueante (I/O de rede/disco).
    """
    log(f"Buscando último 'last_update' para {dataset_id}.{table_id}")
    bd.config.billing_project_id = "rj-iplanrio"
    bd.config.from_file = True
    query = f"""
        SELECT
            last_update
        FROM `{dataset_id}`.`{table_id}`
        ORDER BY last_update DESC
        LIMIT 1
    """
    result = bd.read_sql(query=query)

    if result is None or result.empty:
        log("Nenhum 'last_update' encontrado.")
        return None

    last_update = result["last_update"][0]
    log(f"Último 'last_update' encontrado: {last_update}")
    return last_update


@task
def fetch_history_data(
    last_update: str,
    session_timeout_seconds: Optional[int] = 3600,
    use_whatsapp_format: bool = True,
) -> Optional[str]:
    """
    Ponto de entrada SÍNCRONO que orquestra a execução da lógica assíncrona,
    seguindo o padrão da `outra task`.
    """

    # --- Início da lógica assíncrona interna ---
    async def _main_async_runner() -> Optional[str]:
        log("Criando instância de GoogleAgentEngineHistory...")
        history_instance = await GoogleAgentEngineHistory.create()

        log(f"Buscando histórico a partir de: {last_update or 'início dos tempos'}.")
        data_path = await history_instance.get_history_bulk_from_last_update(
            last_update=last_update,
            session_timeout_seconds=session_timeout_seconds,
            use_whatsapp_format=use_whatsapp_format,
        )

        return data_path

    final_data_path = asyncio.run(_main_async_runner())
    return final_data_path
