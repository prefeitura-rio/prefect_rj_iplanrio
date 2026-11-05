"""Tests for deployment environment boundaries."""

import os
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).parents[1]
PIPELINE = "rj_crm__disparo_template"
BLOCKED_STAGING_PIPELINE = "rj_crm__relatorio_engajamento_hsm"


def plan(
    environment: str,
    ref: str | None,
    output: Path,
    pipeline: str = PIPELINE,
) -> subprocess.CompletedProcess[str]:
    """Run the deployment plan with one pipeline and Git ref."""
    env = {**os.environ, "GITHUB_OUTPUT": str(output)}
    if ref is None:
        env.pop("GITHUB_REF", None)
    else:
        env["GITHUB_REF"] = ref

    return subprocess.run(
        [
            "nu",
            "scripts/deploy.nu",
            "plan",
            "--environment",
            environment,
            "--pipeline",
            pipeline,
            "--after",
            "HEAD",
        ],
        cwd=REPO,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


@pytest.mark.parametrize("ref", ["refs/heads/master", None])
def test_prod_plan_accepts_master_or_a_local_shell(ref: str | None, tmp_path: Path) -> None:
    """Select prod from master or a local shell without an allow list."""
    output = tmp_path / "output"

    result = plan("prod", ref, output)

    assert result.returncode == 0, result.stderr
    assert f'{{"pipeline":["{PIPELINE}"]}}' in output.read_text()


def test_prod_plan_rejects_a_staging_branch(tmp_path: Path) -> None:
    """Regression: Reject a production plan from a staging branch."""
    result = plan("prod", "refs/heads/staging/main", tmp_path / "output")

    assert result.returncode != 0
    assert "Only master deploys to prod" in result.stderr


def test_prod_run_rejects_a_staging_branch() -> None:
    """Regression: Reject a production deployment from a staging branch."""
    result = subprocess.run(
        ["nu", "scripts/deploy.nu", "run", PIPELINE, "--environment", "prod", "--dry-run"],
        cwd=REPO,
        env={**os.environ, "GITHUB_REF": "refs/heads/staging/main"},
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert "Only master deploys to prod" in result.stderr


def test_staging_plan_still_uses_the_allow_list(tmp_path: Path) -> None:
    """Reject a staging deployment that the staging allow list excludes."""
    result = plan(
        "staging",
        "refs/heads/staging/main",
        tmp_path / "output",
        BLOCKED_STAGING_PIPELINE,
    )

    assert result.returncode != 0
    assert "not in .allow.json for staging" in result.stderr
