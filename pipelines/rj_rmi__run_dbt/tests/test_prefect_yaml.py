"""Testes das convenções do prefect.yaml do rj_rmi__run_dbt.

O STYLEGUIDE diz que o CI valida as tags (§13.5), mas nenhum workflow faz
isso. Estes testes conferem o que vale para os dois deployments.
"""

import re
from pathlib import Path

import pytest
import yaml

ENVIRONMENTS = ["staging", "prod"]


@pytest.fixture(scope="module")
def deployments() -> dict[str, dict]:
    """Lê os deployments do ``prefect.yaml``, pelo ambiente no fim do nome.

    :returns: Os deployments, indexados por ``staging`` e ``prod``.
    """
    path = Path(__file__).resolve().parents[1] / "prefect.yaml"
    config = yaml.safe_load(path.read_text(encoding="utf-8"))
    return {
        deployment["name"].rsplit("--", 1)[-1]: deployment
        for deployment in config["deployments"]
    }


@pytest.mark.parametrize("environment", ENVIRONMENTS)
def test_deployment_name_uses_only_lowercase_and_hyphens(
    deployments: dict[str, dict], environment: str
) -> None:
    """Confere o nome do deployment no padrão do §9.1."""
    name = deployments[environment]["name"]
    assert name == f"rj-rmi--run-dbt--{environment}"


@pytest.mark.parametrize("environment", ENVIRONMENTS)
def test_deployment_has_the_required_tags(
    deployments: dict[str, dict], environment: str
) -> None:
    """Confere as três tags obrigatórias do §13."""
    tags = deployments[environment]["tags"]
    assert f"environment:{environment}" in tags
    assert any(
        re.fullmatch(r"severity:(low|medium|high|critical)", tag)
        for tag in tags
    )
    assert any(re.fullmatch(r"code_owner:[\w-]+", tag) for tag in tags)


@pytest.mark.parametrize("environment", ENVIRONMENTS)
def test_deployment_runs_the_pipeline_flow(
    deployments: dict[str, dict], environment: str
) -> None:
    """Confere o entrypoint e o comando da pod, como no §9.2."""
    deployment = deployments[environment]
    entrypoint = "pipelines/rj_rmi__run_dbt/flow.py:rj_rmi__run_dbt"
    command = "uv run --package rj_rmi__run_dbt -- prefect flow-run execute"
    assert deployment["entrypoint"] == entrypoint
    assert deployment["work_pool"]["job_variables"]["command"] == command


def test_only_prod_is_scheduled(deployments: dict[str, dict]) -> None:
    """Confere que só o prod tem schedule, no fuso de São Paulo (§8 e §9.3)."""
    assert "schedules" not in deployments["staging"]
    timezones = {s["timezone"] for s in deployments["prod"]["schedules"]}
    assert timezones == {"America/Sao_Paulo"}
