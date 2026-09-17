"""Preconfigured logging with OpenTelemetry integration for all pipelines.

Every pipeline must obtain its logger through :func:`get_logger` rather than
calling :func:`logging.getLogger` directly. This ensures that OpenTelemetry
export, log format, and any future workspace-wide configuration are applied
uniformly across all pipelines.

Usage::

    from prefect_rj_iplanrio.logging import get_logger

    logger = get_logger(__name__)

    logger.info("Processing %d records", count)
    logger.warning("Retrying after transient error")
    logger.error("Upload failed: %s", error)
"""

import logging
from logging import Logger


def get_logger(name: str) -> Logger:
    """Return a pre-configured logger for the given module.

    :param name: Module name — pass ``__name__`` from the calling module.
    :returns: A :class:`logging.Logger` instance with workspace-wide
        configuration applied.
    """
    return logging.getLogger(name)
