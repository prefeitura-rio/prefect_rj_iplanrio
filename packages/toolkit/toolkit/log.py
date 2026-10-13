"""Toolkit logger built on the Prefect logging tree."""

from prefect.logging import get_logger

from .settings import settings

logger = get_logger("toolkit")
logger.setLevel(settings.LOG_LEVEL)
