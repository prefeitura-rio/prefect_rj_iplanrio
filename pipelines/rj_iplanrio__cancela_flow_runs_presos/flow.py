"""Flow for rj_iplanrio__cancela_flow_runs_presos."""

from prefect import flow

from pipelines.rj_iplanrio__cancela_flow_runs_presos.tasks import cancel_stuck_runs_task


@flow(log_prints=True)
def rj_iplanrio__cancela_flow_runs_presos(max_pending_minutes: int = 360, dry_run: bool = False) -> None:
    cancel_stuck_runs_task(max_pending_minutes=max_pending_minutes, dry_run=dry_run)
