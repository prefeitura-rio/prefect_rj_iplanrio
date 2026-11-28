"""Tests for `.github/scripts/validate_prefect_tags.py`.

Run with: uv run --with pytest --with pyyaml pytest .github/scripts/tests
"""

import importlib.util
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "validate_prefect_tags.py"


def _load_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("validate_prefect_tags", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


vpt = _load_module()

VALID_TAGS = """    tags:
      - environment:{env}
      - severity:medium
      - code_owner:fulano-dev
"""


def _deployment(name: str, env: str, *, tags: str | None = None, extra: str = "") -> str:
    tag_block = VALID_TAGS.format(env=env) if tags is None else tags
    return f"""  - name: {name}
{tag_block}    entrypoint: pipelines/rj_sec__pipe/flow.py:rj_sec__pipe
{extra}"""


def _prefect_yaml(*deployments: str) -> str:
    return "name: rj_sec__pipe\ndeployments:\n" + "".join(deployments)


UNTAGGED_STAGING = _deployment("rj-sec--pipe--staging", "staging", tags="")
UNTAGGED_PROD = _deployment("rj-sec--pipe--prod", "prod", tags="")
TAGGED_STAGING = _deployment("rj-sec--pipe--staging", "staging")
TAGGED_PROD = _deployment("rj-sec--pipe--prod", "prod")


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(["git", *args], cwd=repo, capture_output=True, text=True, check=True)
    return result.stdout.strip()


@pytest.fixture
def repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    _git(tmp_path, "init", "-q", "-b", "master")
    _git(tmp_path, "config", "user.email", "teste@example.com")
    _git(tmp_path, "config", "user.name", "Teste")
    _git(tmp_path, "config", "commit.gpgsign", "false")
    monkeypatch.chdir(tmp_path)
    return tmp_path


def _commit(repo: Path, files: dict[str, str], message: str) -> str:
    for relative, content in files.items():
        path = repo / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        _git(repo, "add", relative)
    _git(repo, "commit", "-q", "-m", message)
    return _git(repo, "rev-parse", "HEAD")


PIPE = "pipelines/rj_sec__pipe/prefect.yaml"


# --- validate_tags -----------------------------------------------------------------------------------------


def test_valid_tags_have_no_problems() -> None:
    deployment = {"name": "x", "tags": ["environment:prod", "severity:critical", "code_owner:MariaLuizaCw"]}
    assert vpt.validate_tags(deployment) == []


def test_missing_tags_report_the_three_keys() -> None:
    problems = vpt.validate_tags({"name": "x"})
    assert len(problems) == 3
    for key in ("environment", "severity", "code_owner"):
        assert any(f"`{key}:<valor>`" in problem for problem in problems)


def test_legacy_bare_tags_are_not_enough() -> None:
    problems = vpt.validate_tags({"name": "x", "tags": ["prod"]})
    assert len(problems) == 3


@pytest.mark.parametrize(
    ("tags", "fragment"),
    [
        (["environment:dev", "severity:low", "code_owner:abc"], "`environment:dev` é inválido"),
        (["environment:prod", "severity:urgent", "code_owner:abc"], "`severity:urgent` é inválido"),
        (["environment:prod", "severity:low", "code_owner:github_account"], "`code_owner:github_account`"),
        (["environment:prod", "severity:low", "code_owner:"], "`code_owner:` é inválido"),
        (["environment:prod", "severity:low", "code_owner:-abc"], "`code_owner:-abc` é inválido"),
        (["environment:prod", "environment:staging", "severity:low", "code_owner:abc"], "mais de um valor"),
    ],
)
def test_invalid_values_are_reported(tags: list[str], fragment: str) -> None:
    problems = vpt.validate_tags({"name": "x", "tags": tags})
    assert len(problems) == 1
    assert fragment in problems[0]


def test_tags_must_be_a_list() -> None:
    assert vpt.validate_tags({"name": "x", "tags": "environment:prod"}) == [
        "`tags` precisa ser uma lista de strings `chave:valor`"
    ]


def test_non_string_tag_is_reported() -> None:
    problems = vpt.validate_tags({"name": "x", "tags": [1, "environment:prod", "severity:low", "code_owner:abc"]})
    assert problems == ["tag 1 não é uma string `chave:valor`"]


# --- touched_deployments -----------------------------------------------------------------------------------


def test_only_new_or_changed_deployments_are_touched() -> None:
    base = {"deployments": [{"name": "a", "x": 1}, {"name": "b", "x": 1}]}
    head = {"deployments": [{"name": "a", "x": 1}, {"name": "b", "x": 2}, {"name": "c"}]}
    assert [name for name, _ in vpt.touched_deployments(base, head)] == ["b", "c"]


def test_every_deployment_is_touched_without_base() -> None:
    head = {"deployments": [{"name": "a"}, {"name": "b"}]}
    assert [name for name, _ in vpt.touched_deployments(None, head)] == ["a", "b"]


# --- run (git) ---------------------------------------------------------------------------------------------


def test_new_pipeline_without_tags_fails(repo: Path) -> None:
    base = _commit(repo, {"README.md": "x\n"}, "base")
    _commit(repo, {PIPE: _prefect_yaml(UNTAGGED_STAGING, UNTAGGED_PROD)}, "nova pipeline")
    violations = vpt.run(base, "HEAD")
    assert {v.deployment for v in violations} == {"rj-sec--pipe--staging", "rj-sec--pipe--prod"}
    assert len(violations) == 6
    assert vpt.main(["--base", base]) == 1


def test_new_pipeline_with_tags_passes(repo: Path) -> None:
    base = _commit(repo, {"README.md": "x\n"}, "base")
    _commit(repo, {PIPE: _prefect_yaml(TAGGED_STAGING, TAGGED_PROD)}, "nova pipeline")
    assert vpt.run(base, "HEAD") == []
    assert vpt.main(["--base", base]) == 0


def test_editing_an_untagged_deployment_fails_only_for_that_deployment(repo: Path) -> None:
    base = _commit(repo, {PIPE: _prefect_yaml(UNTAGGED_STAGING, UNTAGGED_PROD)}, "legado")
    edited_prod = _deployment("rj-sec--pipe--prod", "prod", tags="", extra="    description: nova\n")
    _commit(repo, {PIPE: _prefect_yaml(UNTAGGED_STAGING, edited_prod)}, "edita prod")
    violations = vpt.run(base, "HEAD")
    assert {v.deployment for v in violations} == {"rj-sec--pipe--prod"}
    assert all(v.path == PIPE for v in violations)


def test_untouched_legacy_deployments_pass(repo: Path) -> None:
    legacy = _prefect_yaml(UNTAGGED_STAGING, UNTAGGED_PROD)
    base = _commit(repo, {PIPE: legacy}, "legado")
    # Same deployments, only the build section and a comment change: nothing to validate.
    _commit(repo, {PIPE: "build: []\n# comentário\n" + legacy}, "muda o build")
    assert vpt.run(base, "HEAD") == []


def test_unrelated_change_passes(repo: Path) -> None:
    base = _commit(repo, {PIPE: _prefect_yaml(UNTAGGED_STAGING, UNTAGGED_PROD)}, "legado")
    _commit(repo, {"pipelines/rj_sec__pipe/flow.py": "print('x')\n"}, "muda o flow")
    assert vpt.run(base, "HEAD") == []


def test_backfilling_tags_passes(repo: Path) -> None:
    base = _commit(repo, {PIPE: _prefect_yaml(UNTAGGED_STAGING, UNTAGGED_PROD)}, "legado")
    _commit(repo, {PIPE: _prefect_yaml(TAGGED_STAGING, TAGGED_PROD)}, "backfill")
    assert vpt.run(base, "HEAD") == []


def test_removing_tags_from_a_deployment_fails(repo: Path) -> None:
    base = _commit(repo, {PIPE: _prefect_yaml(TAGGED_STAGING, TAGGED_PROD)}, "com tags")
    _commit(repo, {PIPE: _prefect_yaml(TAGGED_STAGING, UNTAGGED_PROD)}, "remove tags")
    assert {v.deployment for v in vpt.run(base, "HEAD")} == {"rj-sec--pipe--prod"}


def test_invalid_yaml_fails(repo: Path) -> None:
    base = _commit(repo, {"README.md": "x\n"}, "base")
    _commit(repo, {PIPE: "deployments: [\n"}, "yaml quebrado")
    violations = vpt.run(base, "HEAD")
    assert len(violations) == 1
    assert violations[0].message.startswith("YAML inválido")


def test_only_pipeline_prefect_yaml_files_are_checked(repo: Path) -> None:
    base = _commit(repo, {"README.md": "x\n"}, "base")
    untagged = _prefect_yaml(UNTAGGED_STAGING)
    _commit(repo, {"templates/x/prefect.yaml": untagged, "pipelines/rj_sec__pipe/sub/prefect.yaml": untagged}, "fora")
    assert vpt.run(base, "HEAD") == []


def test_changes_on_the_base_branch_are_not_attributed_to_the_pr(repo: Path) -> None:
    _commit(repo, {"README.md": "x\n"}, "base")
    _git(repo, "checkout", "-q", "-b", "feature")
    _commit(repo, {"pipelines/rj_sec__pipe/flow.py": "print('x')\n"}, "feature")
    _git(repo, "checkout", "-q", "master")
    _commit(repo, {PIPE: _prefect_yaml(UNTAGGED_STAGING)}, "legado entra na base depois")
    _git(repo, "checkout", "-q", "feature")
    assert vpt.run("master", "HEAD") == []


def test_unknown_base_returns_exit_code_2(repo: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _commit(repo, {"README.md": "x\n"}, "base")
    assert vpt.main(["--base", "nao-existe"]) == 2
    assert "::error" in capsys.readouterr().out


def test_annotation_escapes_special_characters() -> None:
    violation = vpt.Violation("pipelines/a/prefect.yaml", "rj-a--b--prod", "linha 1\nlinha 2 100%")
    assert violation.annotation() == (
        "::error file=pipelines/a/prefect.yaml,title=Tags do deployment rj-a--b--prod::linha 1%0Alinha 2 100%25"
    )
