"""Utils for rj_rmi__run_dbt."""

import base64
import os
import shlex
import tempfile

import git
from dbt.cli.main import dbtRunnerResult
from iplanrio.pipelines_utils.env import getenv_or_action

REPOSITORY = "github.com/prefeitura-rio/queries-rj-rmi.git"
# Chave RJ_RMI_SA do projeto prefect-jobs do Infisical. Chega no
# prefect-jobs-secrets com esse nome, sem prefixo.
SERVICE_ACCOUNT_ENV = "RJ_RMI_SA"
FAILED_STATUSES = ("error", "fail", "runtime error")


def read_service_account_key() -> str:
    """Lê a chave da service account do RMI, em JSON puro ou em base64.

    :returns: A chave em JSON.
    :raises ValueError: Se ``RJ_RMI_SA`` estiver ausente ou vazia.
    """
    key = os.getenv(SERVICE_ACCOUNT_ENV, "").strip()
    if not key:
        found = sorted(
            name for name in os.environ if "RMI" in name.upper().split("_")
        )
        raise ValueError(
            f"{SERVICE_ACCOUNT_ENV} ausente ou vazia. "
            f"Variáveis com RMI no nome: {found}"
        )
    if not key.startswith("{"):
        key = base64.b64decode(key).decode()
    return key


def set_application_credentials(key: str) -> None:
    """Grava a chave num arquivo e aponta o ADC do Google para ele.

    :param key: Chave da service account, em JSON.
    """
    # O mkstemp cria o arquivo com permissão 600.
    fd, path = tempfile.mkstemp(prefix="rj-rmi-sa-", suffix=".json")
    with os.fdopen(fd, "w", encoding="utf-8") as file:
        file.write(key)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path


def clone_repository() -> tuple[str, str]:
    """Clona o ``master`` do queries-rj-rmi numa pasta temporária.

    :returns: O caminho do clone e o commit clonado, abreviado.
    :raises ValueError: Se ``GITHUB_TOKEN`` não existir.
    """
    path = tempfile.mkdtemp(prefix="queries-rj-rmi-")
    token = getenv_or_action("GITHUB_TOKEN")
    repo = git.Repo.clone_from(
        f"https://{token}@{REPOSITORY}", path, depth=1, branch="master"
    )
    return path, repo.head.commit.hexsha[:7]


def isolate_dbt_environment(project_dir: str) -> None:
    """Troca os ``DBT_*`` herdados da pod pelos caminhos do clone.

    O secret da pod é compartilhado com outros runners dbt, e os ``DBT_*``
    dele não valem aqui. Fica só o ``DBT_USER``, que dá o prefixo dos
    datasets de dev no ``dbt_project.yml`` do queries-rj-rmi. Os caminhos
    vão pelo ambiente porque o dbt valida ``DBT_PROFILES_DIR`` antes de
    receber os que o runner passa.

    :param project_dir: Raiz do clone, onde ficam ``dbt_project.yml`` e
        ``profiles.yml``.
    """
    inherited = [
        name
        for name in os.environ
        if name.startswith("DBT_") and name != "DBT_USER"
    ]
    for name in inherited:
        del os.environ[name]
    os.environ.update(DBT_PROJECT_DIR=project_dir, DBT_PROFILES_DIR=project_dir)


def dbt_args(command: str, select: str, flag: str, target: str) -> list[str]:
    """Monta os argumentos do comando dbt, com ``--target`` sempre explícito.

    :param command: Comando dbt, como ``build`` ou ``source freshness``.
    :param select: Valor do ``--select``. Vazio, a opção não é passada.
    :param flag: Demais argumentos, separados como no shell.
    :param target: Target do ``profiles.yml``.
    :returns: Os argumentos do ``PrefectDbtRunner.invoke``.
    """
    args = [*shlex.split(command), "--target", target, *shlex.split(flag)]
    if select:
        args.extend(["--select", select])
    return args


def failed_node_ids(result: dbtRunnerResult) -> list[str]:
    """Devolve o ``unique_id`` dos nós que terminaram com erro ou falha.

    Os resultados de ``build``, ``run``, ``test`` e ``source freshness``
    trazem ``.node``. Os de ``run-operation`` são ``RunResultOutput``, com
    ``unique_id`` direto. Comandos sem resultado por nó, como ``debug``, não
    têm ``.results``.

    :param result: Retorno do ``PrefectDbtRunner.invoke``.
    :returns: Os ``unique_id``, na ordem do dbt. WARN não entra.
    """
    node_results = getattr(result.result, "results", [])
    return [
        getattr(item, "node", item).unique_id
        for item in node_results
        if item.status in FAILED_STATUSES
    ]
