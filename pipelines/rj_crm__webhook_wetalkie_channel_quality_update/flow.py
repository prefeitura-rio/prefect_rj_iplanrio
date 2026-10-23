"""Flow for rj_crm__webhook_wetalkie_channel_quality_update."""

from prefect import flow

from pipelines.rj_crm__webhook_wetalkie_channel_quality_update.helpers import build_message
from utils.flows import send_alerts


@flow(log_prints=True)
async def rj_crm__webhook_wetalkie_channel_quality_update(query: str, test_mode: bool = False) -> None:
    """Send to Discord one alert for each row that the query returns."""
    await send_alerts(query, build_message, test_mode=test_mode)
