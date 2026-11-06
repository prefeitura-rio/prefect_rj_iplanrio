"""Envie alertas opcionais sobre cargas parciais."""

from typing import Any

import requests

from pipelines.rj_crm__agent_quality_registry.constants import DISCORD_WEBHOOK_URL_ERRORS
from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)


def notify_partial_load(summary: dict[str, Any], webhook_url: str = DISCORD_WEBHOOK_URL_ERRORS) -> bool:
    """Notifique o canal de erros quando a ingestão terminar parcialmente."""
    if not webhook_url:
        logger.warning("Alerta externo não enviado: DISCORD_WEBHOOK_URL_ERRORS não configurado")
        return False
    message = (
        "⚠️ Agent Quality Registry terminou com carga parcial\n"
        f"processados={summary.get('processed', 0)} | "
        f"ignorados={summary.get('skipped', 0)} | "
        f"rejeitados={summary.get('rejected', 0)} | "
        f"falhas_grid={summary.get('grid_failures', 0)} | "
        f"falhas_dead_letter={summary.get('rejection_persist_failures', 0)}"
    )
    try:
        response = requests.post(webhook_url, json={"content": message}, timeout=30)
        response.raise_for_status()
    except requests.RequestException as error:
        logger.error("Falha ao enviar alerta de carga parcial: %s", error)
        return False
    return True
