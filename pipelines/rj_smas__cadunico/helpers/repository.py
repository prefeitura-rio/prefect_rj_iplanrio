"""Commit the generated models to the queries repository."""

from collections.abc import Mapping
from pathlib import Path
from tempfile import TemporaryDirectory

from git import Actor, GitCommandError, PushInfo, Repo
from toolkit.dbt import clone_repository, fill_token, find_project_dir

from pipelines.rj_smas__cadunico.constants import COMMIT_AUTHOR, MODELS_FOLDER


def push_models(repository: str, branch: str, files: Mapping[str, str], message: str) -> bool:
    """Write the files in the models folder of a clone, commit them and push them. Return `False` if nothing changed.

    `files` has the text of each file, by name. The clone is temporary, and it has the
    branch. A `GITHUB_TOKEN` in the URL of the repository becomes the token of the setting.
    Raise `RuntimeError` if the clone or the push fails. The error has no cause, because
    the message of git can hold the token of the URL.
    """
    with TemporaryDirectory(prefix="cadunico_models_") as directory:
        clone = clone_repository(fill_token(repository), Path(directory) / "repository", branch)
        folder = find_project_dir(clone) / MODELS_FOLDER
        folder.mkdir(parents=True, exist_ok=True)

        for name, text in files.items():
            (folder / name).write_text(text, encoding="utf-8")

        repo = Repo(clone)

        if not repo.is_dirty(untracked_files=True):
            return False

        repo.index.add([str(folder / name) for name in files])
        author = Actor(*COMMIT_AUTHOR)
        repo.index.commit(message, author=author, committer=author)

        try:
            results = repo.remote("origin").push(repo.active_branch.name)
        except GitCommandError:
            raise RuntimeError("Push of the models failed") from None

        errors = [result.summary.strip() for result in results if result.flags & PushInfo.ERROR]

        if errors:
            raise RuntimeError(f"Push of the models failed: {errors[0]}")

    return True
