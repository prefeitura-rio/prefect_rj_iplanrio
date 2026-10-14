from pathlib import Path
from types import SimpleNamespace
from typing import ClassVar

import git
import pytest
from pydantic import ValidationError

from toolkit import dbt
from toolkit.dbt import DbtRun

REPOSITORY = "https://example.invalid/queries.git"


class FakeRunner:
    """A `PrefectDbtRunner` that records the settings and the arguments."""

    instances: ClassVar[list["FakeRunner"]] = []
    error: ClassVar[Exception | None] = None

    def __init__(self, **kwargs: object) -> None:
        """Record the keyword arguments and register the instance."""
        self.kwargs = kwargs
        self.invoked: list[list[str]] = []
        FakeRunner.instances.append(self)

    @property
    def settings(self):
        """Return the settings that the runner got."""
        return self.kwargs["settings"]

    def invoke(self, arguments: list[str]) -> SimpleNamespace:
        """Record the arguments, and fail if an error is set."""
        self.invoked.append(arguments)
        if FakeRunner.error is not None:
            raise FakeRunner.error
        return SimpleNamespace(success=True)


@pytest.fixture
def fake_runner(monkeypatch, tmp_path):
    """Replace the dbt runner, and clone into a folder of the test."""
    project = tmp_path / "queries"
    project.mkdir()
    FakeRunner.instances = []
    FakeRunner.error = None
    monkeypatch.setattr(dbt, "PrefectDbtRunner", FakeRunner)
    monkeypatch.setattr(
        dbt, "clone_repository", lambda repository, destination: project
    )
    return FakeRunner


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        pytest.param({}, ["run", "--target", "dev"], id="defaults"),
        pytest.param(
            {"command": "test", "target": "prod"},
            ["test", "--target", "prod"],
            id="command_and_target",
        ),
        pytest.param(
            {"select": "a", "exclude": "b", "state": "prod", "full_refresh": True},
            [
                *("run", "--target", "dev"),
                *("--select", "a", "--exclude", "b", "--state", "prod"),
                "--full-refresh",
            ],
            id="every_option",
        ),
        pytest.param(
            {"command": "source freshness"},
            ["source", "freshness", "--target", "dev"],
            id="source_freshness_is_two_words",
        ),
        pytest.param(
            {"command": "deps", "select": "a", "target": "prod"},
            ["deps"],
            id="a_command_without_options_drops_them",
        ),
    ],
)
def test_the_command_line_of_a_request(values, expected):
    """Regression: each call mutated the shared word list of a two-word command."""
    request = DbtRun(repository=REPOSITORY, **values)

    assert [request.arguments(), request.arguments()] == [expected, expected]


@pytest.mark.parametrize(
    ("values", "field"),
    [
        pytest.param({"repository": ""}, "repository", id="empty_repository"),
        pytest.param({"command": "rn"}, "command", id="unknown_command"),
        pytest.param({"unknown": "x"}, "unknown", id="unknown_field"),
    ],
)
def test_a_request_with_a_bad_field_is_rejected_when_built(values, field):
    """Reject a request with a bad field when it is built."""
    with pytest.raises(ValidationError, match=field):
        DbtRun.model_validate({"repository": REPOSITORY, **values})


def test_dbt_runs_deps_then_the_command_in_the_cloned_project(fake_runner, tmp_path):
    """Regression: the clone was ignored, so dbt ran in the working directory."""
    dbt.execute_dbt(DbtRun(repository=REPOSITORY, select="vendas"))

    [runner] = fake_runner.instances
    assert runner.settings.project_dir == tmp_path / "queries"
    assert runner.invoked == [
        ["deps"],
        ["run", "--target", "dev", "--select", "vendas"],
    ]


def test_a_failed_dbt_command_raises(fake_runner):
    """Raise if a dbt command fails."""
    fake_runner.error = ValueError("dbt failed")

    with pytest.raises(ValueError, match="dbt failed"):
        dbt.execute_dbt(DbtRun(repository=REPOSITORY))


def make_repository(path: Path, *, queries: bool) -> Path:
    """Create a git repository with a dbt project.

    The project is in `queries` or at the root.
    """
    repository = git.Repo.init(path)
    project = path / "queries" if queries else path
    project.mkdir(exist_ok=True)
    (project / "dbt_project.yml").write_text("name: test\n")
    repository.index.add([str(project / "dbt_project.yml")])
    actor = git.Actor("tester", "tester@example.com")
    repository.index.commit("init", author=actor, committer=actor)
    return path


@pytest.mark.parametrize(
    ("queries", "expected"),
    [
        pytest.param(True, "clone/queries", id="the_queries_folder"),
        pytest.param(False, "clone", id="the_repository_root"),
    ],
)
def test_the_dbt_project_is_the_queries_folder_when_there_is_one(
    queries, expected, tmp_path
):
    """Use the `queries` folder as the dbt project if there is one."""
    origin = make_repository(tmp_path / "origin", queries=queries)

    clone = dbt.clone_repository(str(origin), tmp_path / "clone")

    assert dbt.dbt_project_dir(clone) == tmp_path / expected


def test_a_failed_clone_is_reported(tmp_path):
    """Report a failed clone."""
    with pytest.raises(RuntimeError, match="clone failed"):
        dbt.clone_repository(str(tmp_path / "missing"), tmp_path / "clone")
