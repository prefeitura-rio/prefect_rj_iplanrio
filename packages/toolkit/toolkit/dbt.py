"""Run dbt commands from a cloned queries repository."""

import tempfile
from pathlib import Path
from typing import ClassVar

from git import GitCommandError, Repo
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings
from pydantic import BaseModel, ConfigDict

from .constants import COMMAND_WORDS, SELECTABLE
from .telemetry import logger
from .types import DbtCommand, NonEmptyString


class DbtRun(BaseModel):
    """One dbt command of a pipeline."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid", frozen=True)

    repository: NonEmptyString
    command: DbtCommand = "run"
    target: str = "dev"
    select: str = ""
    exclude: str = ""
    state: str = ""
    full_refresh: bool = False

    def arguments(self) -> list[str]:
        """Return the dbt CLI arguments, in the order dbt expects.

        The target and the selection are dropped for a command that takes none,
        such as ``deps``.

        :returns: The arguments, without the ``dbt`` word itself.
        """
        arguments = list(COMMAND_WORDS.get(self.command, (self.command,)))

        if self.command not in SELECTABLE:
            return arguments

        arguments += ["--target", self.target]

        for option, value in (
            ("--select", self.select),
            ("--exclude", self.exclude),
            ("--state", self.state),
        ):
            if value:
                arguments += [option, value]

        if self.full_refresh:
            arguments.append("--full-refresh")

        return arguments


def clone_repository(repository: str, destination: Path) -> Path:
    """Clone the repository into ``destination`` and return the cloned path.

    :param repository: URL of the repository.
    :param destination: Directory to clone into. It must not exist yet.
    :returns: The path of the clone.
    :raises RuntimeError: If the clone fails.
    """
    try:
        Repo.clone_from(repository, destination)
    except GitCommandError as error:
        raise RuntimeError("Repository clone failed") from error

    logger.info("Repository cloned: path=%s", destination)
    return destination


def dbt_project_dir(repository_path: Path) -> Path:
    """Return the dbt project inside a cloned repository.

    :param repository_path: Path of the clone.
    :returns: The ``queries`` folder when it exists, otherwise ``repository_path``.
    """
    queries_path = repository_path / "queries"
    return queries_path if queries_path.is_dir() else repository_path


def execute_dbt(request: DbtRun) -> None:
    """Clone the repository, install dependencies and run one dbt command.

    The clone lives in a temporary directory for the length of the call.

    :param request: The repository and the dbt command.
    :raises RuntimeError: If the clone fails.
    :raises ValueError: If a dbt command fails.
    """
    with tempfile.TemporaryDirectory(prefix="dbt_repository_") as directory:
        project_dir = dbt_project_dir(
            clone_repository(request.repository, Path(directory) / "repository")
        )

        settings = PrefectDbtSettings(project_dir=project_dir)
        runner = PrefectDbtRunner(settings=settings)
        runner.invoke(["deps"])
        runner.invoke(request.arguments())
        logger.info(
            "dbt command finished: command=%s project=%s", request.command, project_dir
        )
