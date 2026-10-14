"""Structured logs on the Prefect logger tree."""

from collections.abc import Mapping

from prefect.logging import get_logger

from .settings import settings
from .types import LogValue

logger = get_logger("toolkit")
logger.setLevel(settings.LOG_LEVEL)


def info(message: str, fields: Mapping[str, LogValue]) -> None:
    """Log at INFO with fields.

    The Prefect UI shows only the message, so the fields are in it as ``key=value``.
    They are also attributes of the record, so an OpenTelemetry log has them as fields.

    :param message: What happened, with no values in it.
    :param fields: The values. A name must not be one of ``logging.LogRecord``.
    :raises KeyError: If a field name is an attribute of ``logging.LogRecord``.
    """
    readable = " ".join(f"{key}={value}" for key, value in fields.items())
    text = f"{message}: {readable}" if readable else message
    logger.info("%s", text, extra=dict(fields))
