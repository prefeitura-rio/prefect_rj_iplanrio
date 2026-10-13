"""Run dbt commands from a cloned queries repository."""

import shutil
from pathlib import Path

import git
from prefect_dbt import PrefectDbtRunner

from .log import logger


def download_repository(git_repository_path: str, base: Path | None = None) -> Path:
    """Clone the dbt repository into ``dbt_repository`` and return the project path.

    :param git_repository_path: URL of the repository.
    :param base: Parent directory. Defaults to the current directory.
    :returns: The ``queries`` folder when it exists, otherwise the repository root.
    :raises ValueError: If the URL is empty.
    :raises RuntimeError: If the clone fails.
    """
    if not git_repository_path:
        raise ValueError("git_repository_path is required")

    repository_path = (base or Path.cwd()) / "dbt_repository"
    shutil.rmtree(repository_path, ignore_errors=True)
    repository_path.mkdir(parents=True)
    try:
        git.Repo.clone_from(git_repository_path, repository_path)
    except git.GitCommandError as error:
        raise RuntimeError("Repository clone failed") from error
    logger.info("Repository downloaded: path=%s", repository_path)

    queries_path = repository_path / "queries"
    return queries_path if queries_path.is_dir() else repository_path


def build_command(
    command: str,
    target: str = "dev",
    select: str = "",
    exclude: str = "",
    state: str = "",
    flag: str = "",
) -> list[str]:
    """Build the dbt argument list.

    :param command: ``run``, ``test``, ``build``, ``source freshness``, ``seed``,
        ``snapshot``, ``deps`` or another dbt command.
    :param target: dbt target.
    :param select: ``--select`` value.
    :param exclude: ``--exclude`` value.
    :param state: ``--state`` value.
    :param flag: One extra flag.
    """
    arguments = ["source", "freshness"] if command == "source freshness" else [command]
    if command in {"build", "run", "test", "source freshness", "seed", "snapshot"}:
        arguments += ["--target", target]
        for option, value in (
            ("--select", select),
            ("--exclude", exclude),
            ("--state", state),
        ):
            if value:
                arguments += [option, value]
        if flag:
            arguments.append(flag)
    return arguments


def execute_dbt(
    command: str = "run",
    target: str = "dev",
    select: str = "",
    exclude: str = "",
    state: str = "",
    flag: str = "",
    git_repository_path: str = "https://github.com/prefeitura-rio/queries-rj-iplanrio",
) -> None:
    """Clone the repository, install dependencies and run one dbt command.

    :raises RuntimeError: If the dbt command fails.
    """
    download_repository(git_repository_path)
    arguments = build_command(command, target, select, exclude, state, flag)
    runner = PrefectDbtRunner(raise_on_failure=False)
    runner.invoke(["deps"])
    result = runner.invoke(arguments)
    logger.info("dbt command finished: command=%s success=%s", command, result.success)
    if not result.success:
        raise RuntimeError(f"dbt command failed: {' '.join(arguments)}")
