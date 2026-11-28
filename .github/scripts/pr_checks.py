# /// script
# requires-python = ">=3.13,<3.14"
# dependencies = [
#     "ruff==0.12.5",
# ]
# ///
"""Pull request checks: ruff on the lines a change adds and the tests of the pipelines it touches.

The base branch still carries ruff findings, so `lint` only fails on findings located in lines the
change adds or edits (plus syntax errors anywhere in a changed file). `tests` runs `pytest` in every
changed pipeline that has a `tests/` directory, and in all of them when the change touches code
shared by the workspace (`src/`, the root `pyproject.toml`, `uv.lock`).

Usage:
    uv run .github/scripts/pr_checks.py lint --base <base revision> [--head <head revision>]
    uv run .github/scripts/pr_checks.py tests --base <base revision> [--head <head revision>]
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tomllib
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
LINTED_PREFIXES = ("pipelines/", "src/", ".github/scripts/")
SHARED_PATHS = ("src/", "pyproject.toml", "uv.lock")
# `pipelines/<pipeline>/<file>`: a path with fewer parts is not inside a pipeline directory.
PIPELINE_FILE_DEPTH = 2
GIT_ERROR_EXIT = 2
HUNK_HEADER = re.compile(r"^@@ -\d+(?:,\d+)? \+(?P<start>\d+)(?:,(?P<count>\d+))? @@")


class GitError(RuntimeError):
    """Raised when a git command fails."""


@dataclass(frozen=True)
class Finding:
    """One ruff finding."""

    path: str
    row: int
    column: int
    code: str | None
    message: str

    def annotation(self) -> str:
        """Format the finding as a GitHub Actions error annotation."""
        title = _escape_property(f"ruff {self.code or 'syntax-error'}")
        location = f"file={_escape_property(self.path)},line={self.row},col={self.column}"
        return f"::error {location},title={title}::{_escape_data(self.message)}"


def _escape_data(value: str) -> str:
    return value.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def _escape_property(value: str) -> str:
    return _escape_data(value).replace(":", "%3A").replace(",", "%2C")


def _git(*args: str) -> str:
    result = subprocess.run(["git", *args], capture_output=True, text=True, check=False, cwd=REPO_ROOT)
    if result.returncode != 0:
        raise GitError(f"git {' '.join(args)} falhou: {result.stderr.strip()}")
    return result.stdout


def merge_base(base: str, head: str) -> str:
    """Return the merge base of `base` and `head`."""
    return _git("merge-base", base, head).strip()


def changed_paths(base: str, head: str) -> list[str]:
    """List the paths added, modified or deleted between `base` and `head`."""
    output = _git("diff", "--name-only", "--no-renames", base, head)
    return sorted(line for line in output.splitlines() if line)


def parse_added_lines(diff: str) -> dict[str, set[int]]:
    """Map each file of a `git diff -U0` output to the line numbers (in the new version) it adds or edits."""
    added: dict[str, set[int]] = {}
    current: str | None = None
    for line in diff.splitlines():
        if line.startswith("+++ "):
            target = line[4:]
            current = target[2:] if target.startswith("b/") else None
            if current is not None:
                added.setdefault(current, set())
            continue
        match = HUNK_HEADER.match(line)
        if match is None or current is None:
            continue
        start = int(match["start"])
        count = 1 if match["count"] is None else int(match["count"])
        added[current].update(range(start, start + count))
    return {path: rows for path, rows in added.items() if rows}


def is_linted(path: str) -> bool:
    """Tell whether a changed file is covered by the lint gate."""
    return path.endswith(".py") and path.startswith(LINTED_PREFIXES)


def added_python_lines(base: str, head: str) -> dict[str, set[int]]:
    """Return the lines each linted Python file adds or edits between `base` and `head`."""
    diff = _git("diff", "-U0", "--no-renames", "--no-color", "--diff-filter=AM", base, head, "--", "*.py")
    return {path: rows for path, rows in parse_added_lines(diff).items() if is_linted(path)}


def parse_ruff_output(output: str, root: Path) -> list[Finding]:
    """Parse the JSON output of `ruff check --output-format json`."""
    findings: list[Finding] = []
    entries: list[dict[str, Any]] = json.loads(output or "[]")
    for entry in entries:
        filename = Path(entry["filename"])
        path = filename.relative_to(root).as_posix() if filename.is_absolute() else filename.as_posix()
        location = entry.get("location") or {}
        findings.append(
            Finding(
                path=path,
                row=int(location.get("row", 1)),
                column=int(location.get("column", 1)),
                code=entry.get("code"),
                message=str(entry.get("message", "")),
            )
        )
    return findings


def findings_on_added_lines(findings: Iterable[Finding], added: dict[str, set[int]]) -> list[Finding]:
    """Keep the findings on lines the change adds or edits, and every syntax error of a changed file."""
    kept = [
        finding
        for finding in findings
        if finding.path in added and (finding.code is None or finding.row in added[finding.path])
    ]
    return sorted(kept, key=lambda finding: (finding.path, finding.row, finding.column))


def run_ruff(paths: Sequence[str]) -> list[Finding]:
    """Run ruff, with the configuration of the root `pyproject.toml`, on `paths`."""
    ruff = shutil.which("ruff")
    if ruff is None:
        raise RuntimeError("ruff não encontrado no PATH: rode o script com `uv run`, que instala a versão fixada")
    result = subprocess.run(
        [ruff, "check", "--no-fix", "--output-format", "json", "--exit-zero", *paths],
        capture_output=True,
        text=True,
        check=False,
        cwd=REPO_ROOT,
    )
    if result.returncode != 0:
        raise RuntimeError(f"ruff falhou (saída {result.returncode}): {result.stderr.strip()}")
    return parse_ruff_output(result.stdout, REPO_ROOT)


def lint(base: str, head: str) -> int:
    """Fail when ruff reports findings on the lines the change adds or edits."""
    added = added_python_lines(merge_base(base, head), head)
    if not added:
        print("Nenhum arquivo Python alterado em pipelines/, src/ ou .github/scripts/.")
        return 0
    findings = findings_on_added_lines(run_ruff(sorted(added)), added)
    for finding in findings:
        print(finding.annotation())
        print(f"{finding.path}:{finding.row}:{finding.column}: {finding.code or 'syntax-error'} {finding.message}")
    print(f"{len(findings)} achado(s) do ruff nas linhas alteradas de {len(added)} arquivo(s).")
    return 1 if findings else 0


def pipelines_with_tests(root: Path) -> list[str]:
    """List the pipeline directories that have a `tests/` directory."""
    return sorted(path.parent.name for path in (root / "pipelines").glob("*/tests") if path.is_dir())


def select_pipelines(paths: Iterable[str], with_tests: Sequence[str]) -> list[str]:
    """Pick the pipelines whose tests a change must run."""
    paths = list(paths)
    if any(path == shared or path.startswith(shared) for path in paths for shared in SHARED_PATHS):
        return sorted(with_tests)
    touched = {
        parts[1]
        for parts in (PurePosixPath(path).parts for path in paths)
        if len(parts) > PIPELINE_FILE_DEPTH and parts[0] == "pipelines"
    }
    return sorted(touched.intersection(with_tests))


def package_name(root: Path, pipeline: str) -> str:
    """Read the workspace package name of a pipeline from its `pyproject.toml`."""
    with (root / "pipelines" / pipeline / "pyproject.toml").open("rb") as file:
        return str(tomllib.load(file)["project"]["name"])


def tests(base: str, head: str) -> int:
    """Run the tests of the pipelines the change touches."""
    selected = select_pipelines(changed_paths(merge_base(base, head), head), pipelines_with_tests(REPO_ROOT))
    if not selected:
        print("Nenhuma pipeline com tests/ alterada.")
        return 0
    # The script runs in its own environment; `uv run --package` must use the workspace one instead.
    workspace_env = {key: value for key, value in os.environ.items() if key != "VIRTUAL_ENV"}
    failed: list[str] = []
    for pipeline in selected:
        package = package_name(REPO_ROOT, pipeline)
        command = [
            "uv",
            "run",
            "--package",
            package,
            "--with",
            "pytest",
            "python",
            "-m",
            "pytest",
            f"pipelines/{pipeline}/tests",
        ]
        print(f"::group::{pipeline}: {' '.join(command)}", flush=True)
        returncode = subprocess.run(command, check=False, cwd=REPO_ROOT, env=workspace_env).returncode
        print("::endgroup::", flush=True)
        if returncode != 0:
            print(f"::error title=Testes de {pipeline}::pytest saiu com {returncode}", flush=True)
            failed.append(pipeline)
    print(f"Pipelines testadas: {', '.join(selected)}. Com falha: {', '.join(failed) or 'nenhuma'}.")
    return 1 if failed else 0


def main(argv: Sequence[str] | None = None) -> int:
    """Parse the arguments and run the chosen check."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("check", choices=("lint", "tests"))
    parser.add_argument("--base", required=True, help="base revision of the change (e.g. origin/master)")
    parser.add_argument("--head", default="HEAD", help="head revision of the change (default: HEAD)")
    args = parser.parse_args(argv)
    try:
        return lint(args.base, args.head) if args.check == "lint" else tests(args.base, args.head)
    except GitError as error:
        print(f"::error::{_escape_data(str(error))}")
        return GIT_ERROR_EXIT


if __name__ == "__main__":
    sys.exit(main())
