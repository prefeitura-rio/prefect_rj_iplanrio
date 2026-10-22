"""Describe a dbt command, and clone the repository that it runs in."""

from collections.abc import Generator
from contextlib import contextmanager
from os import environ
from pathlib import Path
from shlex import split
from typing import ClassVar
from unittest.mock import patch

from git import GitCommandError, Repo
from pydantic import BaseModel, ConfigDict

from .constants import COMMAND_WORDS, GITHUB_TOKEN_PLACEHOLDER, SELECTABLE
from .settings import GithubSettings
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
    flag: str = ""
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

        arguments += split(self.flag)

        return arguments


def fill_token(repository: str) -> str:
    """Return the repository URL with `GITHUB_TOKEN` in it replaced by the token.

    A URL without the placeholder is public, and it comes back as it is. Raise
    `ValueError` if the URL has the placeholder and the `GITHUB_TOKEN` setting is empty.

    ```python
    fill_token("https://GITHUB_TOKEN@github.com/org/repo.git")
    # "https://<token>@github.com/org/repo.git"
    ```
    """
    if GITHUB_TOKEN_PLACEHOLDER not in repository:
        return repository

    token = GithubSettings().GITHUB_TOKEN.get_secret_value()
    if not token:
        raise ValueError("The repository needs GITHUB_TOKEN, and it is not set")

    return repository.replace(GITHUB_TOKEN_PLACEHOLDER, token)


def clone_repository(repository: str, destination: Path) -> Path:
    """Clone the repository into `destination` and return its path.

    The destination must not exist. Raise `RuntimeError` if the clone fails. The error
    has no cause, because the message of git can hold the token of the URL.
    """
    try:
        Repo.clone_from(repository, destination)
    except GitCommandError:
        raise RuntimeError("Repository clone failed") from None

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
