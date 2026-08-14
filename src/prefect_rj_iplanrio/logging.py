"""Preconfigured logging with OpenTelemetry integration for all pipelines.

Every pipeline must obtain its logger through :func:`get_logger` rather than
calling :func:`logging.getLogger` directly. This ensures that OpenTelemetry
export, log format, and any future workspace-wide configuration are applied
uniformly across all pipelines.

Pipeline labels (code_owner, severity) are automatically included in all logs
via structured logging context.

Usage::

    from prefect_rj_iplanrio.logging import get_logger
    from prefect_rj_iplanrio.labels import set_labels

    logger = get_logger(__name__)
    set_labels(code_owner="username", severity="high")

    logger.info("Processing %d records", count)
    logger.warning("Retrying after transient error")
    logger.error("Upload failed: %s", error)
"""

import logging
from logging import LogRecord, Logger

from prefect_rj_iplanrio.labels import get_labels_dict


class LabelsFilter(logging.Filter):
    """Add pipeline labels to every log record."""

    def filter(self, record: LogRecord) -> bool:
        labels = get_labels_dict()
        for key, value in labels.items():
            if not hasattr(record, key):
                setattr(record, key, value)
        return True


def get_logger(name: str) -> Logger:
    """Return a pre-configured logger with label injection.

    :param name: Module name — pass ``__name__`` from the calling module.
    :returns: A :class:`logging.Logger` instance with workspace-wide
        configuration and label injection applied.
    """
    logger = logging.getLogger(name)

    if not any(isinstance(f, LabelsFilter) for f in logger.filters):
        logger.addFilter(LabelsFilter())

    return logger
