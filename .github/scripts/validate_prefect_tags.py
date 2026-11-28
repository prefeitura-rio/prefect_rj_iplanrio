# /// script
# requires-python = ">=3.13,<3.14"
# dependencies = [
#     "pyyaml>=6.0.3",
# ]
# ///
"""Validate the mandatory deployment tags (STYLEGUIDE.md §13) of the `prefect.yaml` files a change touches.

Only the deployments the change adds or edits are checked: a deployment that is identical in the base
revision is left alone, so the gate does not block unrelated pull requests nor require a backfill of
every pipeline at once.

Usage:
    uv run .github/scripts/validate_prefect_tags.py --base <base revision> [--head <head revision>]
"""

import argparse
import re
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any

from yaml import YAMLError, safe_load

PREFECT_YAML_PATTERN = "pipelines/*/prefect.yaml"
ENVIRONMENTS = ("staging", "prod")
SEVERITIES = ("low", "medium", "high", "critical")
REQUIRED_KEYS = ("environment", "severity", "code_owner")
# GitHub username: 1-39 alphanumerics or single hyphens, not starting or ending with a hyphen.
GITHUB_USERNAME = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9]|-(?=[A-Za-z0-9])){0,38}$")


class GitError(RuntimeError):
    """Raised when a git command fails."""


@dataclass(frozen=True)
class Violation:
    """One problem found in a `prefect.yaml` file."""

    path: str
    deployment: str
    message: str

    def annotation(self) -> str:
        """Format the violation as a GitHub Actions error annotation."""
        title = _escape_property(f"Tags do deployment {self.deployment}")
        return f"::error file={_escape_property(self.path)},title={title}::{_escape_data(self.message)}"


def _escape_data(value: str) -> str:
    return value.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def _escape_property(value: str) -> str:
    return _escape_data(value).replace(":", "%3A").replace(",", "%2C")


def _git(*args: str) -> str:
    result = subprocess.run(["git", *args], capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise GitError(f"git {' '.join(args)} falhou: {result.stderr.strip()}")
    return result.stdout


def changed_prefect_yamls(merge_base: str, head: str) -> list[str]:
    """List the `pipelines/*/prefect.yaml` files added or modified between `merge_base` and `head`."""
    output = _git("diff", "--name-only", "--no-renames", "--diff-filter=AM", merge_base, head)
    paths = [line for line in output.splitlines() if line]
    return sorted(path for path in paths if PurePosixPath(path).match(PREFECT_YAML_PATTERN))


def read_revision(revision: str, path: str) -> str | None:
    """Return the content of `path` at `revision`, or None when the file does not exist there."""
    try:
        return _git("show", f"{revision}:{path}")
    except GitError:
        return None


def _deployments(document: Any) -> list[Any]:
    if not isinstance(document, dict):
        return []
    deployments = document.get("deployments")
    return deployments if isinstance(deployments, list) else []


def _deployment_key(deployment: Any, index: int) -> str:
    if isinstance(deployment, dict) and isinstance(deployment.get("name"), str) and deployment["name"]:
        return deployment["name"]
    return f"#{index + 1} (sem name)"


def touched_deployments(base_document: Any, head_document: Any) -> list[tuple[str, Any]]:
    """Return the deployments of `head_document` that are new or differ from `base_document`."""
    base = {_deployment_key(d, i): d for i, d in enumerate(_deployments(base_document))}
    touched: list[tuple[str, Any]] = []
    for index, deployment in enumerate(_deployments(head_document)):
        key = _deployment_key(deployment, index)
        if key not in base or base[key] != deployment:
            touched.append((key, deployment))
    return touched


def validate_tags(deployment: Any) -> list[str]:
    """Return the problems with the mandatory tags of one deployment (empty when it is valid)."""
    if not isinstance(deployment, dict):
        return ["o deployment não é um mapeamento YAML"]
    tags = deployment.get("tags")
    if tags is None:
        tags = []
    if not isinstance(tags, list):
        return ["`tags` precisa ser uma lista de strings `chave:valor`"]

    values: dict[str, set[str]] = {key: set() for key in REQUIRED_KEYS}
    problems: list[str] = []
    for tag in tags:
        if not isinstance(tag, str):
            problems.append(f"tag {tag!r} não é uma string `chave:valor`")
            continue
        key, separator, value = tag.partition(":")
        if separator and key in values:
            values[key].add(value)

    checks = {
        "environment": (lambda v: v in ENVIRONMENTS, f"um de {', '.join(ENVIRONMENTS)}"),
        "severity": (lambda v: v in SEVERITIES, f"um de {', '.join(SEVERITIES)}"),
        "code_owner": (lambda v: GITHUB_USERNAME.fullmatch(v) is not None, "um username do GitHub"),
    }
    for key in REQUIRED_KEYS:
        is_valid, expected = checks[key]
        found = sorted(values[key])
        if not found:
            problems.append(f"falta a tag `{key}:<valor>` ({expected})")
        elif len(found) > 1:
            problems.append(f"a tag `{key}` aparece com mais de um valor: {', '.join(found)}")
        elif not is_valid(found[0]):
            problems.append(f"`{key}:{found[0]}` é inválido (esperado {expected})")
    return problems


def validate_file(path: str, base_content: str | None, head_content: str) -> list[Violation]:
    """Validate the touched deployments of one `prefect.yaml` file."""
    try:
        head_document = safe_load(head_content)
    except YAMLError as error:
        return [Violation(path, "-", f"YAML inválido: {error}")]
    try:
        base_document = safe_load(base_content) if base_content is not None else None
    except YAMLError:
        base_document = None

    violations: list[Violation] = []
    for name, deployment in touched_deployments(base_document, head_document):
        violations.extend(Violation(path, name, problem) for problem in validate_tags(deployment))
    return violations


def run(base: str, head: str) -> list[Violation]:
    """Validate every `prefect.yaml` file changed between `base` and `head`."""
    merge_base = _git("merge-base", base, head).strip()
    violations: list[Violation] = []
    for path in changed_prefect_yamls(merge_base, head):
        head_content = read_revision(head, path)
        if head_content is None:
            continue
        violations.extend(validate_file(path, read_revision(merge_base, path), head_content))
    return violations


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point: print the violations and return the exit code."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--base", required=True, help="revisão base do PR (ex.: origin/master ou o SHA da base)")
    parser.add_argument("--head", default="HEAD", help="revisão a validar (padrão: HEAD)")
    args = parser.parse_args(argv)

    try:
        violations = run(args.base, args.head)
    except GitError as error:
        print(f"::error title=Validação de tags::{_escape_data(str(error))}")
        return 2

    if not violations:
        print("Tags obrigatórias OK nos deployments alterados (STYLEGUIDE.md §13).")
        return 0
    for violation in violations:
        print(violation.annotation())
        print(f"{violation.path}: {violation.deployment}: {violation.message}", file=sys.stderr)
    print(
        f"{len(violations)} problema(s) de tags nos deployments alterados; veja STYLEGUIDE.md §13.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
