"""Tasks do Prefect da pipeline rj_iplanrio__nf_agent."""

from prefect import task
from prefect.cache_policies import NO_CACHE

from .utils.bifrost import build_client
from .utils.poll import PollSummary, poll_sessions
from .utils.settings import inject_gcp_credentials, load_settings
from .utils.submit import SubmitRequest, SubmitSummary, resolve_origem, submit_pending


@task
def inject_credentials_task() -> None:
    """Configura as credenciais GCP a partir do Infisical."""
    inject_gcp_credentials()


@task(cache_policy=NO_CACHE)
def submit_task(
    origem: str | None, mes_envio: str | None, max_paginas: int | None, versao_processamento: str | None
) -> SubmitSummary:
    """Submete todos os PDFs pendentes da origem (ou da base padrão + mes_envio)."""
    settings = load_settings()
    resolved_origem = resolve_origem(origem, mes_envio, settings)
    request = SubmitRequest(input_uri=resolved_origem, max_pages=max_paginas, processing_version=versao_processamento)
    return submit_pending(build_client(), settings, request)


@task(cache_policy=NO_CACHE)
def poll_task() -> PollSummary:
    """Avança as sessões ativas que terminaram."""
    return poll_sessions(build_client(), load_settings())
