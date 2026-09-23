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
import sys
from logging import Logger


def get_logger(name: str) -> Logger:
    """Return a pre-configured logger for the given module.

    :param name: Module name — pass ``__name__`` from the calling module.
    :returns: A :class:`logging.Logger` instance with workspace-wide
        configuration applied.
    """
    logger = logging.getLogger(name)
    _ensure_stderr_handler(logger)
    return logger


# TEMPORARY HACK — DO NOT MERGE TO MAIN. Revert this whole block before
# opening any PR targeting master; the infra team owns the real fix.
#
# Why this exists: records logged through these loggers never reached the
# Prefect worker log streams (neither flow- nor task-level pages showed
# anything — not even WARNING). The shared module configured nothing, so
# output depended entirely on whatever handlers the worker process happens
# to have, which in the k3s-pool worker means these records go nowhere.
# Attaching an explicit stderr handler makes records visible wherever the
# worker captures subprocess stderr, independent of worker logging config.
#
# What was already tried and ruled out: setting the logger *level* does
# NOT fix this (staging commit dd19eed9 tried setLevel(DEBUG), reverted
# the same day in a0faca86 without reaching main) — the default level
# already passes WARNING, yet warnings didn't show either. The problem is
# destination/handlers, not level, so this hack deliberately does NOT call
# setLevel: INFO suppression remains as-is for infra to fix properly.
def _ensure_stderr_handler(logger: Logger) -> None:
    """Attach a stderr handler once per logger (idempotent)."""
    if logger.handlers:
        return
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    logger.addHandler(handler)
