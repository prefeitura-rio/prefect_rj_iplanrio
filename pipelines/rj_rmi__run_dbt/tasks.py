"""Tasks for rj_rmi__run_dbt."""

import base64
import json
import os
import shlex
import tempfile
from pathlib import Path

import git
from iplanrio.pipelines_utils.env import getenv_or_action
from iplanrio.pipelines_utils.logging import log
from prefect import task
from prefect_dbt import PrefectDbtRunner

REPOSITORY = "github.com/prefeitura-rio/queries-rj-rmi.git"
# Infisical: projeto prefect-jobs, pasta dbt-queries, chave RJ_RMI_SA. A pasta vira o prefixo da variável.
SERVICE_ACCOUNT_ENV = "DBT_QUERIES__RJ_RMI_SA"
FAILED_STATUSES = ("error", "fail", "runtime error")


@task
def setup_credentials_task() -> None:
    """Grava a service account do RMI num arquivo e aponta o ADC do Google para ele.

    O ``profiles.yml`` do queries-rj-rmi usa ``method: oauth``, que lê ``GOOGLE_APPLICATION_CREDENTIALS``.
    A chave pode vir em JSON puro ou em base64.
    """
    key = os.getenv(SERVICE_ACCOUNT_ENV, "").strip()
    if not key:
        found = sorted(name for name in os.environ if "RMI" in name.upper().split("_"))
        raise ValueError(f"{SERVICE_ACCOUNT_ENV} ausente ou vazia. Variáveis com RMI no nome: {found}")
    if not key.startswith("{"):
        key = base64.b64decode(key).decode()
    fd, path = tempfile.mkstemp(prefix="rj-rmi-sa-", suffix=".json")  # criado com permissão 600
    os.close(fd)
    Path(path).write_text(key, encoding="utf-8")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path
    log(f"Credencial GCP: {json.loads(key).get('client_email', 'ADC de usuário')}")


@task
def clone_repository_task() -> str:
    """Clona o ``master`` do queries-rj-rmi e devolve o caminho do clone.

    O token do GitHub não sai desta task, e o GitPython o mascara nas mensagens de erro.
    """
    path = tempfile.mkdtemp(prefix="queries-rj-rmi-")
    token = getenv_or_action("GITHUB_TOKEN")
    repo = git.Repo.clone_from(f"https://{token}@{REPOSITORY}", path, depth=1)
    log(f"{REPOSITORY} clonado no commit {repo.head.commit.hexsha[:7]}")
    return path


@task
def run_dbt_task(project_dir: str, command: str, select: str, flag: str, target: str) -> None:
    """Roda ``dbt deps`` e o comando pedido no clone, e falha se algum nó falhar. WARN não falha.

    :param project_dir: Raiz do clone, onde ficam ``dbt_project.yml`` e ``profiles.yml``.
    :param command: Comando dbt, como ``build`` ou ``source freshness``.
    :param select: Valor do ``--select``. Vazio, a opção não é passada.
    :param flag: Demais argumentos, separados como no shell.
    :param target: Target do ``profiles.yml``.
    :raises RuntimeError: Se algum nó terminar com erro ou falha.
    """
    # O secret da pod é compartilhado com outros runners dbt, e nenhum DBT_* dele vale aqui. Os caminhos
    # vão pelo ambiente porque o dbt valida DBT_PROFILES_DIR antes de receber os que o runner passa.
    for name in [name for name in os.environ if name.startswith("DBT_") and name != "DBT_USER"]:
        del os.environ[name]
    os.environ.update(DBT_PROJECT_DIR=project_dir, DBT_PROFILES_DIR=project_dir)
    # Com raise_on_failure=True, o prefect-dbt 0.7.5 quebra em source freshness.
    runner = PrefectDbtRunner(raise_on_failure=False)
    runner.invoke(["deps"])
    args = [*shlex.split(command), "--target", target, *shlex.split(flag)]
    if select:
        args.extend(["--select", select])
    result = runner.invoke(args)
    if not result.success:
        nodes = getattr(result.result, "results", [])
        failed = [getattr(item, "node", item).unique_id for item in nodes if item.status in FAILED_STATUSES]
        raise RuntimeError(f"dbt {command} terminou com erro: {', '.join(failed) or 'ver o log'}")
