"""Preconfigured logging for all pipelines.

Every pipeline must obtain its logger through :func:`get_logger` rather than
calling :func:`logging.getLogger` directly. This ensures that level, log format
and destination are applied uniformly across all pipelines:

- **Level:** ``INFO`` by default, or the value of ``PREFECT_LOGGING_LEVEL``. A
  level the pipeline has already set on that logger is kept.
- **Format and destination:** the records go to Prefect's console handler on
  the root logger (standard error of the flow run's pod, formatted as
  ``HH:MM:SS.mmm | LEVEL | logger.name - message``). If Prefect has not set up
  logging in the process yet, :func:`get_logger` sets it up the same way Prefect
  itself does on import.
- **Prefect UI:** to also send the records to the flow run's logs in the Prefect
  API, list the pipeline's logger prefix (e.g. ``pipelines.rj_crm__exemplo``)
  in the ``PREFECT_LOGGING_EXTRA_LOGGERS`` environment variable of the
  deployment.

There is no OpenTelemetry integration: no exporter is configured for the pipelines.

Usage::

    from prefect_rj_iplanrio.logging import get_logger

    logger = get_logger(__name__)

    logger.info("Processing %d records", count)
    logger.warning("Retrying after transient error")
    logger.error("Upload failed: %s", error)
"""

import logging
from logging import Logger

from prefect.logging import configuration as prefect_logging_configuration
from prefect.settings import get_current_settings


def get_logger(name: str) -> Logger:
    """Return a pre-configured logger for the given module.

    :param name: Module name — pass ``__name__`` from the calling module.
    :returns: A :class:`logging.Logger` whose ``INFO`` records (or those at the
        ``PREFECT_LOGGING_LEVEL`` threshold) reach the pod's log output.
    """
    if not prefect_logging_configuration.PROCESS_LOGGING_CONFIG:
        prefect_logging_configuration.setup_logging()

    logger = logging.getLogger(name)
    if logger.level == logging.NOTSET:
        logger.setLevel(get_current_settings().logging.level)
    return logger
