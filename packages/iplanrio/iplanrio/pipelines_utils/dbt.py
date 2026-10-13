# -*- coding: utf-8 -*-
import os
import shutil

import git
from prefect import task
from prefect_dbt import PrefectDbtRunner

from iplanrio.pipelines_utils.logging import log


def download_repository(git_repository_path: str) -> str:
    """
    Downloads the repository specified by the REPOSITORY_URL.
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

    # Download repository
    try:
        git.Repo.clone_from(git_repository_path, repository_path)
        log(f"Repository downloaded: {git_repository_path}", level="info")
    except git.GitCommandError as e:
        raise Exception(f"Error when downloading repository: {e}")

    # check for 'queries' folder
    queries_path = os.path.join(repository_path, "queries")
    if os.path.isdir(queries_path):
        log(f"'queries' folder found at: {queries_path}", level="info")
        return queries_path

    return repository_path


@task
def execute_dbt_task(
    command: str = "run",
    target: str = "dev",
    select: str = "",
    exclude: str = "",
    state: str = "",
    flag: str = "",
    git_repository_path: str = "https://github.com/prefeitura-rio/queries-rj-iplanrio",
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
    _ = download_repository(git_repository_path=git_repository_path)

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
