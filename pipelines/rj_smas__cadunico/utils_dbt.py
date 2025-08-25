# -*- coding: utf-8 -*-
# ruff: noqa
import os
import shutil

import git
from iplanrio.pipelines_utils.logging import log
from prefect_dbt import PrefectDbtRunner


def get_github_token() -> str:
    """
    Obtém o GitHub token da variável de ambiente.

    Returns:
        str: GitHub token

    Raises:
        ValueError: Se o token não for encontrado
    """
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise ValueError("GITHUB_TOKEN environment variable is required but not set")
    return token


def download_repository(git_repository_path: str, branch: str = "master") -> str:
    """
    Downloads the repository specified by the REPOSITORY_URL from a specific branch.

    Args:
        git_repository_path (str): URL do repositório Git
        branch (str): Nome da branch a ser baixada (default: "master")

    Returns:
        str: Caminho para a pasta queries ou para o repositório
    """
    if not git_repository_path:
        raise ValueError("git_repository_path is required")

    # Create repository folder
    try:
        repository_path = os.path.join(os.getcwd(), "dbt_repository")

        if os.path.exists(repository_path):
            shutil.rmtree(repository_path, ignore_errors=False)
        os.makedirs(repository_path)

        log(f"Repository folder created: {repository_path}", level="info")

    except Exception as e:
        raise Exception(f"Error when creating repository folder: {e}")

    # Download repository from specific branch
    try:
        repo = git.Repo.clone_from(git_repository_path, repository_path, branch=branch)
        log(
            f"Repository downloaded: {git_repository_path} (branch: {branch})",
            level="info",
        )
        log(f"Current branch: {repo.active_branch.name}", level="info")
    except git.GitCommandError as e:
        raise Exception(f"Error when downloading repository from branch '{branch}': {e}")

    # check for 'queries' folder
    queries_path = os.path.join(repository_path, "queries")
    if os.path.isdir(queries_path):
        log(f"'queries' folder found at: {queries_path}", level="info")
        return queries_path

    return str(repository_path)


def push_models_to_branch(
    repository_path: str,
    github_token: str,
    commit_message: str = "feat: update CadUnico models",
    author_name: str = "pipeline_cadunico",
    author_email: str = "pipeline@prefeitura.rio",
) -> bool:
    """
    Faz commit e push dos modelos criados para a branch ativa usando GitHub token.

    Args:
        repository_path (str): Caminho para o repositório local
        github_token (str): GitHub token para autenticação (obrigatório)
        commit_message (str): Mensagem do commit
        author_name (str): Nome do author
        author_email (str): Email do author

    Returns:
        bool: True se o push foi bem-sucedido
    """
    if not github_token:
        log("GitHub token is required for push operations", level="error")
        return False
    try:
        # Encontrar o diretório raiz do repositório
        if "queries" in repository_path:
            repo_root = os.path.dirname(repository_path)
        else:
            repo_root = repository_path

        # Abrir o repositório
        repo = git.Repo(repo_root)

        # Configurar autor se necessário
        try:
            with repo.config_writer() as git_config:
                git_config.set_value("user", "name", author_name)
                git_config.set_value("user", "email", author_email)
        except Exception:
            log("Could not set git config, using existing config", level="warning")

        # Verificar branch ativa
        current_branch = repo.active_branch.name
        log(f"Current branch: {current_branch}", level="info")

        # Verificar se há mudanças
        if repo.is_dirty() or repo.untracked_files:
            # Adicionar arquivos modificados e novos
            repo.git.add(A=True)  # Equivale a git add -A

            # Listar arquivos que serão commitados
            staged_files = (
                repo.git.diff("--cached", "--name-only").split("\n") if repo.git.diff("--cached", "--name-only") else []
            )
            untracked_files = repo.untracked_files

            all_changes = staged_files + untracked_files
            log(f"Files to be committed ({len(all_changes)}):", level="info")
            for file_path in all_changes[:10]:  # Mostrar primeiros 10 arquivos
                log(f"  - {file_path}", level="info")
            if len(all_changes) > 10:
                log(f"  ... and {len(all_changes) - 10} more files", level="info")

            # Fazer commit
            commit = repo.index.commit(commit_message)
            log(f"Commit created: {commit.hexsha[:8]} - {commit_message}", level="info")

            # Configurar URL remota com token para push
            origin = repo.remote("origin")
            original_url = origin.url

            # Criar URL com token para push
            if original_url.startswith("https://github.com/"):
                repo_path = original_url.replace("https://github.com/", "")
                auth_url = f"https://{github_token}@github.com/{repo_path}"
                origin.set_url(auth_url)
                log("Configured remote URL with GitHub token for push", level="info")

            # Fazer push
            push_info = origin.push(current_branch)

            # Restaurar URL original (sem token) por segurança
            origin.set_url(original_url)

            # Verificar resultado do push
            for info in push_info:
                if info.flags & info.ERROR:
                    log(
                        f"Error pushing to {current_branch}: {info.summary}",
                        level="error",
                    )
                    return False
                else:
                    log(
                        f"Successfully pushed to {current_branch}: {info.summary}",
                        level="info",
                    )

            log(
                f"✅ Models successfully pushed to branch '{current_branch}'",
                level="info",
            )
            return True

        else:
            log("No changes to commit", level="info")
            return True

    except git.GitCommandError as e:
        log(f"Git command error: {e}", level="error")
        return False
    except Exception as e:
        log(f"Error pushing models: {e}", level="error")
        return False


def execute_dbt(
    command: str = "run",
    target: str = "dev",
    select: str = "",
    exclude: str = "",
    state: str = "",
    flag: str = "",
):
    """
    Executes a dbt command using PrefectDbtRunner from prefect-dbt.

    Args:
        command (str): DBT command to execute (run, test, build, source freshness, deps, etc.)
        target (str): DBT target environment (dev, prod, etc.)
        select (str): DBT select argument for filtering models
        exclude (str): DBT exclude argument for filtering models
        state (str): DBT state argument for incremental processing
        flag (str): Additional DBT flags

    Returns:
        PrefectDbtResult: Result of the DBT command execution
    """
    # Build the command arguments
    if command == "source freshness":
        command_args = ["source", "freshness"]
    else:
        command_args = [command]

    # Add common arguments for most DBT commands
    if command in ("build", "run", "test", "source freshness", "seed", "snapshot"):
        command_args.extend(["--target", target])

        if select:
            command_args.extend(["--select", select])
        if exclude:
            command_args.extend(["--exclude", exclude])
        if state:
            command_args.extend(["--state", state])
        if flag:
            command_args.extend([flag])

    log(f"Executing dbt command: {' '.join(command_args)}", level="info")

    # Initialize PrefectDbtRunner
    runner = PrefectDbtRunner(
        raise_on_failure=False  # Allow the flow to handle failures gracefully
    )
    # Execute the dbt deps command
    try:
        deps_result = runner.invoke(["deps"])
        log("✅ DBT dependencies installed successfully", level="info")
        log(msg=str(deps_result))
    except Exception as e:
        log(f"❌ Error installing DBT dependencies: {e}", level="error")
        raise

    # Execute the dbt command with the constructed arguments
    try:
        running_result = runner.invoke(command_args)
        log(
            f"DBT command completed with success: {running_result.success}",
            level="info",
        )
    except Exception as e:
        log(f"Error executing DBT command: {e}", level="error")
        raise

    log(msg=str(running_result))
