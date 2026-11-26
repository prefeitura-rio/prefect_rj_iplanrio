"""Tasks que cancelam flow runs presos em Pending."""

from datetime import UTC, datetime, timedelta

from prefect import task
from prefect.client.orchestration import get_client

from pipelines.rj_iplanrio__cancela_flow_runs_presos.utils import runs


@task
def cancel_stuck_runs_task(max_pending_minutes: int, dry_run: bool) -> list[str]:
    """Cancele os flow runs presos em Pending há mais de ``max_pending_minutes``.

    :param max_pending_minutes: Tempo máximo aceito em Pending, em minutos.
    :param dry_run: Quando ``True``, só registra no log o que seria cancelado.
    :returns: Ids dos runs cancelados (ou que seriam, em ``dry_run``).
    """
    with get_client(sync_client=True) as client:
        return runs.cancel_stuck_runs(
            client,
            now=datetime.now(UTC),
            max_pending=timedelta(minutes=max_pending_minutes),
            dry_run=dry_run,
        )
