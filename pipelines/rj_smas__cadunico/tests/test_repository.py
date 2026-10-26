from pathlib import Path
from typing import cast

import pytest
from git import Actor, Repo

from pipelines.rj_smas__cadunico.constants import MODELS_FOLDER
from pipelines.rj_smas__cadunico.helpers.repository import push_models


@pytest.fixture
def origin(tmp_path: Path) -> Path:
    """Return a bare repository that has the branch `cadunico` and a file."""
    work = Repo.init(tmp_path / "work")
    (tmp_path / "work" / "README").write_text("x\n")
    work.index.add(["README"])
    actor = Actor("tester", "tester@example.com")
    work.index.commit("init", author=actor, committer=actor)
    work.create_head("cadunico")
    Repo.clone_from(str(tmp_path / "work"), tmp_path / "origin.git", bare=True)

    return tmp_path / "origin.git"


def test_the_models_are_committed_and_pushed_to_the_branch(origin: Path, tmp_path: Path) -> None:
    """Push the files to the models folder of the branch, as the pipeline."""
    pushed = push_models(str(origin), "cadunico", {"a.sql": "select 1\n", "a.yml": "version: 2\n"}, "feat: models")

    head = Repo(origin).commit("cadunico")
    assert pushed
    assert head.message == "feat: models"
    assert head.author.name == "CadUnico Pipeline"
    check = Repo.clone_from(str(origin), tmp_path / "check", branch="cadunico")
    assert (Path(cast("str", check.working_dir)) / MODELS_FOLDER / "a.sql").read_text() == "select 1\n"


def test_a_second_push_of_the_same_files_changes_nothing(origin: Path) -> None:
    """Return `False` when the files are the same as the ones on the branch."""
    files = {"a.sql": "select 1\n"}
    push_models(str(origin), "cadunico", files, "first")

    assert not push_models(str(origin), "cadunico", files, "second")
    assert Repo(origin).commit("cadunico").message == "first"


def test_a_changed_file_is_a_new_commit(origin: Path) -> None:
    """Commit again when a file changes."""
    push_models(str(origin), "cadunico", {"a.sql": "select 1\n"}, "first")

    assert push_models(str(origin), "cadunico", {"a.sql": "select 2\n"}, "second")
    assert Repo(origin).commit("cadunico").message == "second"


def test_a_missing_repository_is_reported(tmp_path: Path) -> None:
    """Raise `RuntimeError` for a clone that fails."""
    with pytest.raises(RuntimeError, match="clone failed"):
        push_models(str(tmp_path / "missing"), "cadunico", {"a.sql": "x"}, "m")
