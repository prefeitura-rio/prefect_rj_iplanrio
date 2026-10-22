"""Flow for rj_rmi__run_dbt."""

from prefect import flow
from toolkit.dbt import DbtRun

from pipelines.rj_rmi__run_dbt.tasks import setup_credentials_task
from utils.hooks import notify_failure
from utils.tasks import run_dbt


@flow(flow_run_name="dbt-{request.command}-{request.target}", log_prints=True, on_failure=[notify_failure])
async def rj_rmi__run_dbt(*, request: DbtRun, report: bool = False) -> None:
    """Run one dbt command of the RMI queries repository as the service account of the RMI.

    Set `request` in the `prefect.yaml`, as the `dbt_run` flow takes it. In a pod, only the
    target `prod` works: the service account has no role in `rj-rmi-dev`.
    """
    setup_credentials_task()
    await run_dbt(request, report=report)
