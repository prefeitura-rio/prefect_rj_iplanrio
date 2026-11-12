"""Task da guarda contra a extração das notas nacionais; a decisão e a consulta ao Oracle ficam em ``utils/dfen.py``."""

from iplanrio.pipelines_utils.logging import log
from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils import dfen, oracle


@task(cache_policy=NO_CACHE)
def check_dfen_extraction_task(infisical_secret_path: str, enabled: bool, stale_hours: int) -> None:
    """Recusa a execução se a aplicação Nota Carioca estiver extraindo notas nacionais para o schema DFEN.

    Consulta ``NFSE_OWNER.TAREFAS`` com a mesma conexão da foto. Tarefas em execução há mais de ``stale_hours`` horas
    são avisadas como travadas e ignoradas; sem privilégio de leitura na tabela, avisa e deixa a carga seguir.

    :param infisical_secret_path: Pasta do segredo do Oracle.
    :param enabled: ``False`` (``check_dfen_extraction=false``) pula a conferência.
    :param stale_hours: Horas em execução a partir das quais uma tarefa é considerada travada.
    :raises DfenExtractionError: Se houver extração em andamento, nomeando as tarefas e como proceder.
    """
    if not enabled:
        log("Conferência da extração DFEN desligada (check_dfen_extraction=false)", "warning")
        return
    config = oracle.read_oracle_config(infisical_secret_path)
    status = dfen.check_no_extraction(
        lambda: dfen.read_running_tasks(config), stale_hours, lambda message: log(message, "warning")
    )
    if status is not None:
        log(f"Nenhuma extração DFEN em andamento no Oracle ({len(status.stale)} tarefa(s) travada(s) ignorada(s))")
