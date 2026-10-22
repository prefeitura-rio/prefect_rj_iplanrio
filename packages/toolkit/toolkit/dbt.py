"""Run dbt commands from a cloned queries repository."""

from collections.abc import Generator
from contextlib import contextmanager
from os import environ
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import ClassVar
from unittest.mock import patch

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

    def build_arguments(self) -> list[str]:
        """Return the dbt CLI arguments in the order dbt expects.

        A command that takes no target or selection, such as `deps`, gets none.

        ```python
        DbtRun(repository="r", select="tag:daily").build_arguments()
        # ["run", "--target", "dev", "--select", "tag:daily"]
        DbtRun(repository="r", command="deps").build_arguments()
        # ["deps"]
        ```
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
    """Clone the repository into `destination` and return its path.

    The destination must not exist. Raise `RuntimeError` if the clone fails.
    """
    try:
        Repo.clone_from(repository, destination)
    except GitCommandError as error:
        raise RuntimeError("Repository clone failed") from error

    logger.info("Repository cloned: path=%s", destination)
    return destination


def find_project_dir(repository_path: Path) -> Path:
    """Return the `queries` folder of a clone if it exists, else the clone path."""
    queries_path = repository_path / "queries"
    return queries_path if queries_path.is_dir() else repository_path


@contextmanager
def use_profiles(project_dir: Path) -> Generator[None]:
    """Point dbt at the `profiles.yml` of the project, if it has one.

    dbt looks in `~/.dbt` otherwise, and it refuses to run if that folder is missing.
    """
    if (project_dir / "profiles.yml").exists():
        with patch.dict(environ, {"DBT_PROFILES_DIR": str(project_dir)}):
            yield
    else:
        yield


def execute_dbt(request: DbtRun) -> None:
    """Clone the repository and run one dbt command in it.

    The clone is temporary, and dbt runs `deps` before the command. A `profiles.yml` in
    the project wins over the one in `~/.dbt`. Raise `RuntimeError` if the clone fails,
    and `ValueError` if dbt fails.
    """
    with TemporaryDirectory(prefix="dbt_repository_") as directory:
        project_dir = find_project_dir(
            clone_repository(request.repository, Path(directory) / "repository")
        )

        with use_profiles(project_dir):
            runner = PrefectDbtRunner(
                settings=PrefectDbtSettings(project_dir=project_dir)
            )
            runner.invoke(["deps"])
            runner.invoke(request.build_arguments())
        logger.info(
            "dbt command finished: command=%s project=%s", request.command, project_dir
        )
