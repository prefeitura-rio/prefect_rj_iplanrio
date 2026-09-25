"""Tasks for rj_rmi__run_dbt."""

from iplanrio.pipelines_utils.env import getenv_or_action
from iplanrio.pipelines_utils.logging import log
from prefect import task
from prefect_dbt import PrefectDbtRunner

from pipelines.rj_rmi__run_dbt import utils
from pipelines.rj_rmi__run_dbt.constants import REPOSITORY


@task
def setup_credentials_task() -> None:
    """Grava a service account do RMI num arquivo e aponta o ADC para ele.

    O ``profiles.yml`` do queries-rj-rmi usa ``method: oauth``, que lê
    ``GOOGLE_APPLICATION_CREDENTIALS``. A chave pode vir em JSON puro ou em
    base64.

    :raises ValueError: Se ``RJ_RMI_SA`` estiver ausente ou vazia.
    """
    key = utils.read_service_account_key()
    utils.set_application_credentials(key)
    identity = utils.credentials_identity(key)
    log(f"Credencial GCP: {identity}")


@task
def clone_repository_task() -> str:
    """Clona o ``master`` do queries-rj-rmi e devolve o caminho do clone.

    O token do GitHub não sai desta task, e o GitPython o mascara nas
    mensagens de erro.

    :returns: O caminho do clone.
    :raises ValueError: Se ``GITHUB_TOKEN`` não existir.
    """
    token = getenv_or_action("GITHUB_TOKEN")
    path, commit = utils.clone_repository(token)
    log(f"{REPOSITORY} clonado no commit {commit}")
    return path


@task
def run_dbt_task(
    project_dir: str, command: str, select: str, flag: str, target: str
) -> None:
    """Roda ``dbt deps`` e o comando pedido, e falha se algum nó falhar.

    WARN não falha.

    :param project_dir: Raiz do clone, onde ficam ``dbt_project.yml`` e
        ``profiles.yml``.
    :param command: Comando dbt, como ``build`` ou ``source freshness``.
    :param select: Valor do ``--select``. Vazio, a opção não é passada.
    :param flag: Demais argumentos, separados como no shell.
    :param target: Target do ``profiles.yml``.
    :raises RuntimeError: Se algum nó terminar com erro ou falha.
    """
    utils.isolate_dbt_environment(project_dir)
    # Com raise_on_failure=True, o prefect-dbt 0.7.5 quebra em source freshness.
    runner = PrefectDbtRunner(raise_on_failure=False)
    runner.invoke(["deps"])
    result = runner.invoke(
        utils.dbt_args(command=command, select=select, flag=flag, target=target)
    )
    if not result.success:
        failed = utils.failed_node_ids(result)
        detail = ", ".join(failed) or "ver o log"
        raise RuntimeError(f"dbt {command} terminou com erro: {detail}")
