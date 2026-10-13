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
    """A ``PrefectDbtRunner`` that records the settings and the arguments."""

    instances: ClassVar[list["FakeRunner"]] = []
    error: ClassVar[Exception | None] = None

    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs
        self.invoked: list[list[str]] = []
        FakeRunner.instances.append(self)

    @property
    def settings(self):
        return self.kwargs["settings"]

    def invoke(self, arguments: list[str]) -> SimpleNamespace:
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
            {
                "select": "a",
                "exclude": "b",
                "state": "prod",
                "full_refresh": True,
            },
            [
                "run",
                "--target",
                "dev",
                "--select",
                "a",
                "--exclude",
                "b",
                "--state",
                "prod",
                "--full-refresh",
            ],
            id="every_option",
        ),
        pytest.param(
            {"command": "source freshness"},
            ["source", "freshness", "--target", "dev"],
            id="source_freshness_is_two_words",
        ),
        pytest.param({"command": "deps"}, ["deps"], id="deps_takes_no_option"),
        pytest.param(
            {"command": "deps", "select": "a", "target": "prod"},
            ["deps"],
            id="options_are_dropped_for_a_command_that_takes_none",
        ),
    ],
)
def test_arguments(values, expected):
    assert DbtRun(repository=REPOSITORY, **values).arguments() == expected


@pytest.mark.parametrize(
    ("command", "expected"),
    [
        pytest.param("run", ["run", "--target", "dev"], id="one_word"),
        pytest.param(
            "source freshness",
            ["source", "freshness", "--target", "dev"],
            id="two_words",
        ),
        pytest.param("deps", ["deps"], id="no_option"),
    ],
)
def test_a_command_word_list_is_not_shared_between_calls(command, expected):
    """Regression: the shared list of a multi-word command used to be mutated."""
    DbtRun(repository=REPOSITORY, command=command).arguments()

    assert DbtRun(repository=REPOSITORY, command=command).arguments() == expected


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        pytest.param({"repository": ""}, "repository", id="empty_repository"),
        pytest.param(
            {"repository": REPOSITORY, "command": "rn"}, "command", id="unknown_command"
        ),
        pytest.param(
            {"repository": REPOSITORY, "unknown": "x"}, "unknown", id="unknown_field"
        ),
    ],
)
def test_the_request_is_validated_when_it_is_built(values, expected):
    """The repository is not empty, the command is known and no field is extra."""
    with pytest.raises(ValidationError, match=expected):
        DbtRun.model_validate(values)


def test_execute_dbt_runs_deps_then_the_command_with_the_cloned_project(
    fake_runner, tmp_path
):
    """Regression: the clone was ignored, so dbt ran in the working directory."""
    dbt.execute_dbt(DbtRun(repository=REPOSITORY, select="vendas"))

    [runner] = fake_runner.instances
    assert runner.settings.project_dir == tmp_path / "queries"
    assert runner.invoked == [
        ["deps"],
        ["run", "--target", "dev", "--select", "vendas"],
    ]


def test_execute_dbt_lets_a_failed_command_raise(fake_runner):
    fake_runner.error = ValueError("dbt failed")
    with pytest.raises(ValueError, match="dbt failed"):
        dbt.execute_dbt(DbtRun(repository=REPOSITORY))


def make_repository(path: Path, *, queries: bool) -> Path:
    """Create a local git repository, optionally with a ``queries`` project inside."""
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
        pytest.param(True, Path("clone/queries"), id="queries_folder"),
        pytest.param(False, Path("clone"), id="repository_root"),
    ],
)
def test_dbt_project_dir(queries, expected, tmp_path):
    origin = make_repository(tmp_path / "origin", queries=queries)

    clone = dbt.clone_repository(str(origin), tmp_path / "clone")
    project = dbt.dbt_project_dir(clone)

    assert project == tmp_path / expected
    assert (project / "dbt_project.yml").is_file()


def test_clone_repository_reports_a_failed_clone(tmp_path):
    with pytest.raises(RuntimeError, match="clone failed"):
        dbt.clone_repository(str(tmp_path / "missing"), tmp_path / "clone")
