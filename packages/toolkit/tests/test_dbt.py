from os import environ

import pytest
from pydantic import ValidationError

from toolkit import dbt
from toolkit.dbt import DbtRun

REPOSITORY = "https://example.invalid/queries.git"


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

    assert [request.build_arguments(), request.build_arguments()] == [
        expected,
        expected,
    ]


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


@pytest.mark.parametrize("has_profile", [True, False])
def test_regression_the_profile_of_the_repository_is_used_when_it_has_one(
    fake_runner, tmp_path, has_profile
):
    """Regression: dbt ignored the `profiles.yml` of the repository."""
    if has_profile:
        (tmp_path / "queries" / "profiles.yml").write_text("p: {}\n")

    dbt.execute_dbt(DbtRun(repository=REPOSITORY))

    [runner] = fake_runner.instances
    assert (runner.settings.profiles_dir == tmp_path / "queries") is has_profile
    assert "DBT_PROFILES_DIR" not in environ


def test_a_failed_dbt_command_raises(fake_runner):
    """Raise if a dbt command fails."""
    fake_runner.error = ValueError("dbt failed")

    with pytest.raises(ValueError, match="dbt failed"):
        dbt.execute_dbt(DbtRun(repository=REPOSITORY))


@pytest.mark.parametrize(
    ("queries", "expected"),
    [
        pytest.param(True, "clone/queries", id="the_queries_folder"),
        pytest.param(False, "clone", id="the_repository_root"),
    ],
)
def test_the_dbt_project_is_the_queries_folder_when_there_is_one(
    queries, expected, tmp_path, make_repository
):
    """Use the `queries` folder as the dbt project if there is one."""
    origin = make_repository(tmp_path / "origin", queries=queries)

    clone = dbt.clone_repository(str(origin), tmp_path / "clone")

    assert dbt.find_project_dir(clone) == tmp_path / expected


def test_a_failed_clone_is_reported(tmp_path):
    """Report a failed clone."""
    with pytest.raises(RuntimeError, match="clone failed"):
        dbt.clone_repository(str(tmp_path / "missing"), tmp_path / "clone")
