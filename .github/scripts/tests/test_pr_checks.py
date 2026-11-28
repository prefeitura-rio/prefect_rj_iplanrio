"""Tests for `.github/scripts/pr_checks.py`.

Run with: uv run --no-project --python 3.13 --with pytest --with ruff==0.12.5 pytest .github/scripts/tests
"""

import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "pr_checks.py"


def _load_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("pr_checks", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


prc = _load_module()

DIFF = """diff --git a/pipelines/rj_a__x/flow.py b/pipelines/rj_a__x/flow.py
index 1111111..2222222 100644
--- a/pipelines/rj_a__x/flow.py
+++ b/pipelines/rj_a__x/flow.py
@@ -3,0 +4,2 @@ import os
+import sys
+import json
@@ -10 +12 @@ def flow():
-    return 1
+    return 2
@@ -20,3 +21,0 @@ def other():
-    a = 1
-    b = 2
-    c = 3
diff --git a/pipelines/rj_a__x/new.py b/pipelines/rj_a__x/new.py
new file mode 100644
index 0000000..3333333
--- /dev/null
+++ b/pipelines/rj_a__x/new.py
@@ -0,0 +1,3 @@
+a = 1
+b = 2
+c = 3
"""


def test_parse_added_lines_maps_hunks_to_new_line_numbers() -> None:
    assert prc.parse_added_lines(DIFF) == {
        "pipelines/rj_a__x/flow.py": {4, 5, 12},
        "pipelines/rj_a__x/new.py": {1, 2, 3},
    }


def test_parse_added_lines_drops_files_with_only_deletions() -> None:
    diff = """--- a/src/x.py
+++ b/src/x.py
@@ -1,2 +0,0 @@
-a = 1
-b = 2
"""
    assert prc.parse_added_lines(diff) == {}


def test_parse_added_lines_ignores_deleted_files() -> None:
    diff = """--- a/src/x.py
+++ /dev/null
@@ -1 +0,0 @@
-a = 1
"""
    assert prc.parse_added_lines(diff) == {}


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("pipelines/rj_a__x/flow.py", True),
        ("src/prefect_rj_iplanrio/logging.py", True),
        (".github/scripts/pr_checks.py", True),
        ("pipelines/rj_a__x/prefect.yaml", False),
        ("templates/x/flow.py", False),
        ("setup.py", False),
    ],
)
def test_is_linted(path: str, expected: bool) -> None:
    assert prc.is_linted(path) is expected


def test_parse_ruff_output_makes_paths_relative_to_the_root(tmp_path: Path) -> None:
    output = json.dumps(
        [
            {
                "filename": str(tmp_path / "pipelines/rj_a__x/flow.py"),
                "location": {"row": 4, "column": 1},
                "code": "F401",
                "message": "`sys` imported but unused",
            },
            {
                "filename": str(tmp_path / "pipelines/rj_a__x/new.py"),
                "location": {"row": 9, "column": 2},
                "code": None,
                "message": "SyntaxError: Expected an expression",
            },
        ]
    )
    assert prc.parse_ruff_output(output, tmp_path) == [
        prc.Finding("pipelines/rj_a__x/flow.py", 4, 1, "F401", "`sys` imported but unused"),
        prc.Finding("pipelines/rj_a__x/new.py", 9, 2, None, "SyntaxError: Expected an expression"),
    ]


def test_parse_ruff_output_accepts_empty_output(tmp_path: Path) -> None:
    assert prc.parse_ruff_output("", tmp_path) == []
    assert prc.parse_ruff_output("[]", tmp_path) == []


def test_findings_on_added_lines_keeps_only_changed_lines_and_syntax_errors() -> None:
    added = {"pipelines/rj_a__x/flow.py": {4, 5, 12}}
    on_added = prc.Finding("pipelines/rj_a__x/flow.py", 5, 1, "F401", "unused")
    old_line = prc.Finding("pipelines/rj_a__x/flow.py", 30, 1, "E722", "bare except")
    syntax = prc.Finding("pipelines/rj_a__x/flow.py", 40, 1, None, "SyntaxError")
    other_file = prc.Finding("pipelines/rj_b__y/flow.py", 5, 1, "F401", "unused")
    kept = prc.findings_on_added_lines([syntax, other_file, old_line, on_added], added)
    assert kept == [on_added, syntax]


def test_annotation_escapes_properties_and_data() -> None:
    finding = prc.Finding("pipelines/a,b/flow.py", 3, 7, "E501", "Line too long (130 > 120)\n100%")
    assert finding.annotation() == (
        "::error file=pipelines/a%2Cb/flow.py,line=3,col=7,title=ruff E501::Line too long (130 > 120)%0A100%25"
    )


WITH_TESTS = ["rj_a__x", "rj_b__y", "rj_c__z"]


@pytest.mark.parametrize(
    ("paths", "expected"),
    [
        (["pipelines/rj_a__x/flow.py", "pipelines/rj_d__w/flow.py"], ["rj_a__x"]),
        (["pipelines/rj_b__y/tests/test_flow.py", "pipelines/rj_b__y/prefect.yaml"], ["rj_b__y"]),
        (["AGENTS.md", ".github/workflows/pr-checks.yaml", "pipelines/README.md"], []),
        (["src/prefect_rj_iplanrio/logging.py"], WITH_TESTS),
        (["uv.lock"], WITH_TESTS),
        (["pyproject.toml"], WITH_TESTS),
        (["pipelines/rj_a__x/pyproject.toml"], ["rj_a__x"]),
    ],
)
def test_select_pipelines(paths: list[str], expected: list[str]) -> None:
    assert prc.select_pipelines(paths, WITH_TESTS) == expected


def test_pipelines_with_tests_and_package_name(tmp_path: Path) -> None:
    (tmp_path / "pipelines/rj_a__x/tests").mkdir(parents=True)
    (tmp_path / "pipelines/rj_a__x/pyproject.toml").write_text('[project]\nname = "rj-a-x"\n')
    (tmp_path / "pipelines/rj_b__y").mkdir(parents=True)
    assert prc.pipelines_with_tests(tmp_path) == ["rj_a__x"]
    assert prc.package_name(tmp_path, "rj_a__x") == "rj-a-x"


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True)


@pytest.fixture
def repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A git repository whose base commit already has a ruff finding in an untouched line."""
    _git(tmp_path, "init", "-q", "-b", "master")
    _git(tmp_path, "config", "user.email", "ci@example.com")
    _git(tmp_path, "config", "user.name", "CI")
    (tmp_path / "pyproject.toml").write_text('[tool.ruff]\nline-length = 120\n[tool.ruff.lint]\nselect = ["E", "F"]\n')
    pipeline = tmp_path / "pipelines/rj_a__x"
    pipeline.mkdir(parents=True)
    (pipeline / "flow.py").write_text("import os\n\n\ndef flow() -> int:\n    return 1\n")
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-q", "-m", "base")
    monkeypatch.setattr(prc, "REPO_ROOT", tmp_path)
    return tmp_path


def test_lint_passes_when_the_change_adds_no_finding(repo: Path) -> None:
    (repo / "pipelines/rj_a__x/flow.py").write_text("import os\n\n\ndef flow() -> int:\n    return 2\n")
    _git(repo, "commit", "-q", "-am", "change")
    assert prc.lint("master~1", "HEAD") == 0


def test_lint_fails_on_a_finding_in_an_added_line(repo: Path, capsys: pytest.CaptureFixture[str]) -> None:
    (repo / "pipelines/rj_a__x/flow.py").write_text("import os\nimport sys\n\n\ndef flow() -> int:\n    return 1\n")
    _git(repo, "commit", "-q", "-am", "change")
    assert prc.lint("master~1", "HEAD") == 1
    out = capsys.readouterr().out
    assert "::error file=pipelines/rj_a__x/flow.py,line=2" in out
    assert "line=1," not in out


def test_lint_skips_changes_without_python_files(repo: Path) -> None:
    (repo / "AGENTS.md").write_text("# AGENTS\n")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "docs")
    assert prc.lint("master~1", "HEAD") == 0


@pytest.mark.usefixtures("repo")
def test_main_reports_git_errors(capsys: pytest.CaptureFixture[str]) -> None:
    assert prc.main(["lint", "--base", "does-not-exist"]) == prc.GIT_ERROR_EXIT
    assert "::error::git merge-base" in capsys.readouterr().out
