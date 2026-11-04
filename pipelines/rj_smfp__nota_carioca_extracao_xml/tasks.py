"""Tasks of rj_smfp__nota_carioca_extracao_xml."""

from asyncio import sleep
from time import monotonic

from httpx2 import AsyncClient
from prefect import task
from prefect.cache_policies import NO_CACHE
from toolkit.executor.sources.protocols import DBConn
from toolkit.telemetry import logger

from pipelines.rj_smfp__nota_carioca_extracao_xml.constants import PROXY_URL
from pipelines.rj_smfp__nota_carioca_extracao_xml.helpers.oracle import OracleTaskStore
from pipelines.rj_smfp__nota_carioca_extracao_xml.helpers.run import run_extraction
from pipelines.rj_smfp__nota_carioca_extracao_xml.helpers.soap import build_tls_context, call_extraction
from pipelines.rj_smfp__nota_carioca_extracao_xml.settings import Settings
from pipelines.rj_smfp__nota_carioca_extracao_xml.types import Clock, ExtractionServices, Limits, RoutineResponse
from utils.dispatch import post_notice


async def notify(message: str, *, enabled: bool) -> None:
    """Post a line to Discord, unless it is off. A failure never fails the run."""
    if not enabled:
        return

    try:
        await post_notice(message)
    except Exception as error:
        logger.warning("The notice to Discord failed: %s", error)


@task(cache_policy=NO_CACHE)
async def run_extraction_task(url: str, limits: Limits, *, discord_notifications: bool) -> str:
    """Run the extraction, and return the summary of what happened.

    The settings are read first, so a missing one fails before any call.
    """
    settings = Settings()
    token = settings.NOTA_CARIOCA_ROTINAS_TOKEN.get_secret_value()
    conn = DBConn(
        settings.DB_HOST,
        int(settings.DB_PORT),
        settings.DB_SERVICE_NAME,
        settings.DB_USERNAME,
        settings.DB_PASSWORD.get_secret_value(),
    )
    logger.info("Extração XML: webservice %s (proxy %s); acompanhamento em NFSE_OWNER.TAREFAS", url, PROXY_URL)

    async with AsyncClient(proxy=PROXY_URL, verify=build_tls_context(), follow_redirects=False) as client:

        async def call() -> RoutineResponse:
            """Call the extraction with the token."""
            return await call_extraction(client, url, token)

        async def announce(message: str) -> None:
            """Post a line, unless Discord is off."""
            await notify(message, enabled=discord_notifications)

        services = ExtractionServices(call, OracleTaskStore(conn), announce, Clock(monotonic, sleep))

        return await run_extraction(services, limits)
