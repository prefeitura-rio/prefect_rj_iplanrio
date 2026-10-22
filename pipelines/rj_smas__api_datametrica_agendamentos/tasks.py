"""Tasks of rj_smas__api_datametrica_agendamentos."""

from datetime import date

from prefect import task
from prefect.deployments import arun_deployment
from pydantic import JsonValue
from toolkit.telemetry import logger

from pipelines.rj_smas__api_datametrica_agendamentos.constants import DISPATCH_DEPLOYMENT
from pipelines.rj_smas__api_datametrica_agendamentos.helpers import build_headers, build_rows
from pipelines.rj_smas__api_datametrica_agendamentos.settings import Settings
from utils.tasks import request_json


@task
async def fetch_appointments_task(target_date: date) -> list[dict[str, JsonValue]]:
    """Fetch the appointments of a date from the Datametrica API, through the proxy of Brazil."""
    settings = Settings()
    url = (
        settings.DATAMETRICA_URL
        if settings.DATAMETRICA_URL.startswith("http")
        else f"http://{settings.DATAMETRICA_URL}"
    )
    target = f"{url.rstrip('/')}/{target_date}"
    data = await request_json(
        f"{settings.PROXY_URL.rstrip('/')}/",
        params={"url": target},
        headers=build_headers(settings.DATAMETRICA_TOKEN.get_secret_value(), settings.PROXY_TOKEN.get_secret_value()),
        timeout=30,
    )
    records = data if isinstance(data, list) else []
    rows = build_rows(records)
    logger.info("Appointments fetched: date=%s count=%d", target_date, len(rows))

    return rows


@task
async def trigger_dispatch_task(parameters: dict[str, JsonValue]) -> None:
    """Start the dispatch flow of the Cadastro Único reminders, without waiting for it."""
    await arun_deployment(name=DISPATCH_DEPLOYMENT, parameters=parameters, timeout=0)
