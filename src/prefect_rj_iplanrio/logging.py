"""Single entry point for logging in all pipelines.

Every pipeline obtains its logger from here, and passes ``"toolkit"``. That gives the one
workspace logger, ``prefect.toolkit``: it has the Prefect formatting, it shows INFO, it
reaches the Prefect UI, and with the OpenTelemetry export on it reaches the collector.
It is the same object as ``toolkit.telemetry.logger``. A plain
:func:`logging.getLogger` logger is outside the ``prefect`` tree, so it has none of that.

Usage::

    from prefect_rj_iplanrio.logging import get_logger

    logger = get_logger("toolkit")

    logger.info("Records processed: count=%d", count)
    logger.warning("Retrying after transient error")
    logger.error("Upload failed: %s", error)
"""

from prefect.logging import get_logger

__all__ = ["get_logger"]
