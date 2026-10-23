"""prefect.yaml: os deployments existem, apontam para flows de verdade e a agenda do sincronizador está inativa."""

import importlib
import inspect
from pathlib import Path

import pytest
import yaml
from prefect.flows import Flow

PACKAGE = Path(__file__).parent.parent
DEPLOYMENTS = {
    deployment["name"]: deployment
    for deployment in yaml.safe_load((PACKAGE / "prefect.yaml").read_text())["deployments"]
}
SYNC = [f"rj-smfp--nota-carioca-contribuintes-postgres--{env}" for env in ("staging", "prod")]
LOADER = [f"rj-smfp--nota-carioca-bq-to-postgres--{env}" for env in ("staging", "prod")]


def test_the_package_has_a_loader_and_a_sync_deployment_per_environment():
    assert sorted(DEPLOYMENTS) == sorted(SYNC + LOADER)


@pytest.mark.parametrize("name", SYNC + LOADER)
def test_the_entrypoint_is_a_flow_of_this_package(name):
    path, function = DEPLOYMENTS[name]["entrypoint"].split(":")
    module = importlib.import_module(path.removesuffix(".py").replace("/", "."))

    assert isinstance(getattr(module, function), Flow)


@pytest.mark.parametrize("name", SYNC + LOADER)
def test_the_deployment_parameters_exist_in_the_flow_signature(name):
    path, function = DEPLOYMENTS[name]["entrypoint"].split(":")
    flow = getattr(importlib.import_module(path.removesuffix(".py").replace("/", ".")), function)

    assert set(DEPLOYMENTS[name].get("parameters", {})) <= set(inspect.signature(flow.fn).parameters)


@pytest.mark.parametrize("name", SYNC)
def test_the_sync_runs_every_15_minutes_but_the_schedule_is_inactive_until_the_gate(name):
    (schedule,) = DEPLOYMENTS[name]["schedules"]

    assert schedule["cron"] == "*/15 * * * *"
    assert schedule["active"] is False


@pytest.mark.parametrize("name", LOADER)
def test_the_loader_has_no_schedule(name):
    assert "schedules" not in DEPLOYMENTS[name]


@pytest.mark.parametrize(
    ("name", "secret"), [(SYNC[0], "prefect-jobs-secrets-staging"), (SYNC[1], "prefect-jobs-secrets")]
)
def test_each_environment_reads_its_own_secret(name, secret):
    assert DEPLOYMENTS[name]["work_pool"]["job_variables"]["secretName"] == secret
