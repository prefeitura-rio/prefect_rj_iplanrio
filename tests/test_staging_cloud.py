from pathlib import Path
from re import compile as compile_pattern
from typing import NotRequired, TypedDict, cast

import pytest
from yaml import safe_load

PIPELINES = Path(__file__).parent.parent / "pipelines"
STAGING_CLOUD = {
    "bucket": "rj-iplanrio-dia-staging",
    "project": "rj-iplanrio-dia",
    "connection": "rj-iplanrio-dia.us.biglake",
}
TAKES_CLOUD = compile_pattern(r"cloud: Cloud|import .*\b(dump_database|dump_arcgis|dump_gsheet|maintain_delta)\b")
FOLDERS = sorted(folder.name for folder in PIPELINES.iterdir() if (folder / "prefect.yaml").is_file())


class Parameters(TypedDict):
    """The parameter of a deployment or a schedule that these tests read."""

    cloud: NotRequired[dict[str, str]]


class Schedule(TypedDict):
    """A schedule of a prod deployment."""

    parameters: NotRequired[Parameters]


class Deployment(TypedDict):
    """A deployment of a `prefect.yaml`."""

    name: str
    parameters: NotRequired[Parameters]
    schedules: NotRequired[list[Schedule]]


def deployments(pipeline: str, environment: str) -> list[Deployment]:
    """Return the deployments of a pipeline for one environment."""
    config = safe_load((PIPELINES / pipeline / "prefect.yaml").read_text())
    found = cast("list[Deployment]", config["deployments"])
    return [deployment for deployment in found if deployment["name"].endswith(f"--{environment}")]


@pytest.mark.parametrize("pipeline", FOLDERS)
def test_a_staging_deployment_writes_to_the_staging_cloud(pipeline: str) -> None:
    """Check that a staging deployment sets the staging cloud if its flow takes one."""
    if not TAKES_CLOUD.search((PIPELINES / pipeline / "flow.py").read_text()):
        pytest.skip("the flow takes no `cloud`")

    staging = deployments(pipeline, "staging")
    assert staging
    assert [deployment.get("parameters", {}).get("cloud") for deployment in staging] == [STAGING_CLOUD] * len(staging)


@pytest.mark.parametrize("pipeline", FOLDERS)
def test_a_prod_deployment_never_uses_the_staging_cloud(pipeline: str) -> None:
    """Check that no prod deployment or schedule names the staging bucket."""
    for deployment in deployments(pipeline, "prod"):
        parameters = [deployment.get("parameters", {})] + [
            schedule.get("parameters", {}) for schedule in deployment.get("schedules", [])
        ]
        clouds = [group["cloud"] for group in parameters if "cloud" in group]
        assert all(cloud["bucket"] != STAGING_CLOUD["bucket"] for cloud in clouds)
