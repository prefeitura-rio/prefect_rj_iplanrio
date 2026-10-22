"""Tasks of rj_rmi__run_dbt."""

from prefect import task

from pipelines.rj_rmi__run_dbt.helpers import write_credentials
from pipelines.rj_rmi__run_dbt.settings import Settings


@task
def setup_credentials_task() -> None:
    """Point the Google ADC at the service account of the RMI.

    The `profiles.yml` of the repository uses `method: oauth`, which reads
    `GOOGLE_APPLICATION_CREDENTIALS`.
    """
    write_credentials(Settings().RMI_SERVICE_ACCOUNT.get_secret_value())
