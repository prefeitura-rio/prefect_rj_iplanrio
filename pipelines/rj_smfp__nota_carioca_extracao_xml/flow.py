"""Flow for rj_smfp__nota_carioca_extracao_xml."""

from prefect import flow

from pipelines.rj_smfp__nota_carioca_extracao_xml.tasks import run_extraction_task
from pipelines.rj_smfp__nota_carioca_extracao_xml.types import Limits

DEFAULT_LIMITS = Limits()


@flow(log_prints=True, flow_run_name="extracao-xml")
async def rj_smfp__nota_carioca_extracao_xml(
    *, url: str, limits: Limits = DEFAULT_LIMITS, discord_notifications: bool = True
) -> None:
    """Start the XML extraction of the Nota Carioca and wait for it to end.

    It calls `ExtrairDocumentosFiscaisNacionaisAutomatico` of the web service at `url`,
    through the squid of the cluster, and follows the task in `NFSE_OWNER.TAREFAS` until
    `DATA_TERMINO` is filled with `STATUS = 2`. Then it waits until no extraction (46)
    or post-processing (48) runs, the condition of the guard of `oracle_to_bq`.
    `Conflict` follows the extraction that runs, and `OK` with no task goes straight to
    the wait. Any other end fails the run, which stops the full load. It never writes to
    Oracle and never stops the task of the application, not even at the timeout.
    """
    await run_extraction_task(url, limits, discord_notifications=discord_notifications)
