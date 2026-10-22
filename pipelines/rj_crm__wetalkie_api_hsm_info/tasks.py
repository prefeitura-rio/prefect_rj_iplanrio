"""Tasks of rj_crm__wetalkie_api_hsm_info."""

from datetime import UTC, datetime

from prefect import task
from pydantic import JsonValue
from toolkit.telemetry import logger

from pipelines.rj_crm__wetalkie_api_hsm_info.constants import FIELDS, LOGIN_URL, MAX_PAGES, PAGE_SIZE, TEMPLATES_URL
from pipelines.rj_crm__wetalkie_api_hsm_info.helpers import build_rows, read_page, read_token
from utils.settings import WetalkieSettings
from utils.tasks import request_json


@task
async def login_task() -> str:
    """Log in to the Wetalkie API and return the token."""
    settings = WetalkieSettings()
    body: dict[str, JsonValue] = {
        "username": settings.WETALKIE_USERNAME,
        "password": settings.WETALKIE_PASSWORD.get_secret_value(),
    }

    return read_token(await request_json(LOGIN_URL, method="POST", body=body, timeout=30))


@task
async def fetch_templates_task(token: str) -> list[dict[str, str | None]]:
    """Fetch every HSM template, active and inactive, and return them as rows."""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    templates: list[dict[str, JsonValue]] = []

    for active in ("true", "false"):
        for page in range(MAX_PAGES + 1):
            data = await request_json(
                f"{TEMPLATES_URL};active={active}",
                params={"fields": FIELDS, "pageNumber": str(page), "pageSize": str(PAGE_SIZE)},
                headers=headers,
                timeout=30,
            )
            found, has_next = read_page(data)
            templates += found

            if not has_next:
                break
        else:
            logger.warning("Page limit reached: active=%s", active)

    logger.info("Templates fetched: count=%d", len(templates))

    return build_rows(templates, datetime.now(UTC))
