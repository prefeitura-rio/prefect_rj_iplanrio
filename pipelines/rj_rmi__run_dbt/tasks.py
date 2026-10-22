"""Tasks for rj_rmi__run_dbt."""

from prefect import task
from prefect_dbt import PrefectDbtRunner
from toolkit.telemetry import logger

from pipelines.rj_rmi__run_dbt import utils
from pipelines.rj_rmi__run_dbt.constants import REPOSITORY
from pipelines.rj_rmi__run_dbt.settings import Settings


@task
def setup_credentials_task() -> None:
    """Grava a service account do RMI num arquivo e aponta o ADC para ele.

    O ``profiles.yml`` do queries-rj-rmi usa ``method: oauth``, que lê
    ``GOOGLE_APPLICATION_CREDENTIALS``.

    :raises pydantic.ValidationError: Se ``RMI_SERVICE_ACCOUNT`` não existir ou estiver vazia.
    """
    utils.set_application_credentials(Settings().RMI_SERVICE_ACCOUNT.get_secret_value())


@task(retries=3, retry_delay_seconds=60)
def clone_repository_task() -> str:
    """Clona o ``master`` do queries-rj-rmi e devolve o caminho do clone.

    O token do GitHub não sai desta task nem das mensagens de erro do clone.
    Uma falha no clone, como uma instabilidade do GitHub, é tentada de novo
    antes de falhar o flow, inclusive a falta do token.

    :returns: O caminho do clone.
    :raises pydantic.ValidationError: Se ``GITHUB_TOKEN`` não existir ou estiver vazia.
    """
    path, commit = utils.clone_repository(Settings().GITHUB_TOKEN.get_secret_value())
    logger.info("Repositório clonado: repository=%s commit=%s", REPOSITORY, commit)
    return path


@task
def run_dbt_task(
    project_dir: str, command: str, select: str, flag: str, target: str
) -> None:
    """Roda ``dbt deps`` e o comando pedido, e falha se algum deles falhar.

    WARN não falha.

    :param project_dir: Raiz do clone, onde ficam ``dbt_project.yml`` e
        ``profiles.yml``.
    :param command: Comando dbt, como ``build`` ou ``source freshness``.
    :param select: Valor do ``--select``. Vazio, a opção não é passada.
    :param flag: Demais argumentos, separados como no shell.
    :param target: Target do ``profiles.yml``.
    :raises RuntimeError: Se o ``deps`` ou algum nó terminar com erro ou
        falha.
    """
    utils.isolate_dbt_environment(project_dir)
    # Com raise_on_failure=True, o prefect-dbt 0.7.5 quebra em source freshness.
    runner = PrefectDbtRunner(raise_on_failure=False)
    deps = runner.invoke(["deps"])
    if not deps.success:
        raise RuntimeError("dbt deps terminou com erro: ver o log")
    result = runner.invoke(
        utils.dbt_args(command=command, select=select, flag=flag, target=target)
    )
    if not result.success:
        failed = utils.failed_node_ids(result)
        detail = ", ".join(failed) or "ver o log"
        raise RuntimeError(f"dbt {command} terminou com erro: {detail}")
