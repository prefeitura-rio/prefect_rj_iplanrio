from os import environ
from pathlib import Path

import pytest
from git import Repo
from pydantic import ValidationError

from toolkit import dbt
from toolkit.dbt import DbtRun, fill_token

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
            {"select": "a", "flag": "--exclude x --vars '{a: 1}'"},
            [
                *("run", "--target", "dev", "--select", "a"),
                *("--exclude", "x", "--vars", "{a: 1}"),
            ],
            id="a_flag_is_split_like_a_shell_line",
        ),
        pytest.param(
            {"command": "deps", "select": "a", "target": "prod"},
            ["deps"],
            id="a_command_without_options_drops_them",
        ),
    ],
)
def test_the_command_line_of_a_request(values, expected) -> None:
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
def test_a_request_with_a_bad_field_is_rejected_when_built(values, field) -> None:
    """Reject a request with a bad field when it is built."""
    with pytest.raises(ValidationError, match=field):
        DbtRun.model_validate({"repository": REPOSITORY, **values})


@pytest.mark.parametrize(
    ("queries", "expected"),
    [
        pytest.param(True, "clone/queries", id="the_queries_folder"),
        pytest.param(False, "clone", id="the_repository_root"),
    ],
)
def test_the_dbt_project_is_the_queries_folder_when_there_is_one(
    queries, expected, tmp_path, make_repository
) -> None:
    """Use the `queries` folder as the dbt project if there is one."""
    origin = make_repository(tmp_path / "origin", queries=queries)

    clone = dbt.clone_repository(str(origin), tmp_path / "clone")

    assert dbt.find_project_dir(clone) == tmp_path / expected


def test_the_clone_has_the_branch_of_the_request(tmp_path, make_repository) -> None:
    """Check out the branch that the request names."""
    origin = make_repository(tmp_path / "origin", queries=False)
    Repo(origin).git.branch("models")

    clone = dbt.clone_repository(str(origin), tmp_path / "clone", "models")

    assert Repo(clone).active_branch.name == "models"


def test_a_failed_clone_is_reported(tmp_path) -> None:
    """Report a failed clone."""
    with pytest.raises(RuntimeError, match="clone failed"):
        dbt.clone_repository(str(tmp_path / "missing"), tmp_path / "clone")


@pytest.mark.parametrize(
    ("repository", "token", "expected"),
    [
        pytest.param(
            "https://GITHUB_TOKEN@github.com/org/repo.git",
            "abc",
            "https://abc@github.com/org/repo.git",
            id="the_placeholder_is_replaced",
        ),
        pytest.param(
            "https://github.com/org/repo.git",
            "",
            "https://github.com/org/repo.git",
            id="a_public_url_needs_no_token",
        ),
    ],
)
def test_the_token_replaces_the_placeholder_of_the_url(
    monkeypatch, repository, token, expected
) -> None:
    """Put the token in a private URL, and leave a public URL as it is."""
    monkeypatch.setenv("GITHUB_TOKEN", token)

    assert fill_token(repository) == expected


def test_a_private_url_without_a_token_is_refused(monkeypatch) -> None:
    """Raise before the clone if the URL needs a token that is not set."""
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)

    with pytest.raises(ValueError, match="GITHUB_TOKEN"):
        fill_token("https://GITHUB_TOKEN@github.com/org/repo.git")


def test_regression_the_error_of_a_failed_clone_leaves_out_the_token(tmp_path) -> None:
    """Regression: the git message, with the URL and its token, reached the logs."""
    with pytest.raises(RuntimeError, match="clone failed") as error:
        dbt.clone_repository(
            f"https://secret-token@{tmp_path}/missing.git", tmp_path / "clone"
        )

    assert error.value.__cause__ is None
    assert error.value.__suppress_context__


PROFILES = """
iplan:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      keyfile: /tmp/credentials.json
      project: rj-iplanrio-dev
    pr:
      type: bigquery
      method: service-account-json
      project: rj-iplanrio-dev
"""

EXPECTED = PROFILES.replace("      keyfile: /tmp/credentials.json\n", "").replace(
    "method: service-account\n", "method: oauth\n"
)


def test_the_profile_of_the_project_logs_in_with_the_default_credentials(
    tmp_path, monkeypatch
) -> None:
    """Use a copy of the profile with `oauth` and without the key file."""
    (tmp_path / "profiles.yml").write_text(PROFILES)
    monkeypatch.delenv("DBT_PROFILES_DIR", raising=False)

    with dbt.use_profiles(tmp_path):
        used = (Path(environ["DBT_PROFILES_DIR"]) / "profiles.yml").read_text()

    assert used == EXPECTED
    assert "keyfile" in (tmp_path / "profiles.yml").read_text()
    assert "DBT_PROFILES_DIR" not in environ


def test_a_project_without_profiles_keeps_the_dbt_folder(tmp_path, monkeypatch) -> None:
    """Leave `DBT_PROFILES_DIR` alone when the project has no profile."""
    monkeypatch.delenv("DBT_PROFILES_DIR", raising=False)

    with dbt.use_profiles(tmp_path):
        assert "DBT_PROFILES_DIR" not in environ
