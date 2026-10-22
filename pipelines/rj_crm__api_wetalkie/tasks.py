"""Tasks of rj_crm__api_wetalkie."""

from asyncio import to_thread

from prefect import task
from pydantic import JsonValue
from toolkit.telemetry import logger

from pipelines.rj_crm__api_wetalkie.constants import ATTENDANCES_PATH
from pipelines.rj_crm__api_wetalkie.helpers import read_items
from utils.audio import transcribe_attendance, transcribe_url
from utils.wetalkie import WetalkieApi, build_attendance_rows


@task
async def fetch_attendances_task() -> list[dict[str, JsonValue]]:
    """Fetch the attendances that the Wetalkie API has to give."""
    async with WetalkieApi.connect() as api:
        items = read_items(await api.get(ATTENDANCES_PATH))

    logger.info("Attendances fetched: count=%d", len(items))

    return items


@task
async def build_rows_task(items: list[dict[str, JsonValue]], max_audio_seconds: int) -> list[dict[str, str | None]]:
    """Transcribe the audio of the attendances and return their rows."""

    def transcribe(url: str) -> str:
        """Transcribe one audio."""
        return transcribe_url(url, max_audio_seconds)

    transcribed = [await to_thread(transcribe_attendance, item, transcribe) for item in items]

    return build_attendance_rows(transcribed)
