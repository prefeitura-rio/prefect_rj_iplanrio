"""Flow for rj_crm__webhook_wetalkie_template_category_update."""

from prefect import flow
from prefect.logging import get_logger

from pipelines.rj_crm__webhook_wetalkie_template_category_update.helpers import build_message
from utils.settings import NoticeSettings
from utils.tasks import notify_discord, read_bq

logger = get_logger("toolkit")


@flow(log_prints=True)
async def rj_crm__webhook_wetalkie_template_category_update(query: str, test_mode: bool = False) -> None:
    """Send to Discord one alert for each row that the query returns."""
    webhook = NoticeSettings().DISCORD_WEBHOOK_URL.get_secret_value()
    rows = (await read_bq(query, project="rj-iplanrio")).to_dicts()

    if not rows:
        logger.info("Nenhum alerta encontrado. Finalizando.")
        return

    for row in rows:
        message = build_message(row)
        if message is None:
            continue
        if test_mode:
            logger.info("Execução de Teste: %s", message)
        else:
            await notify_discord(message, webhook=webhook)
