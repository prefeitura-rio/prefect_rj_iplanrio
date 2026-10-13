"""Single entry point for logging in all pipelines.

Every pipeline must obtain its logger through :func:`get_logger` rather than
calling :func:`logging.getLogger` directly. This is the place to apply any
workspace-wide log configuration (format, destination).

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
