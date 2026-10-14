"""The part of ``opentelemetry-instrumentation-logging`` that the toolkit uses. The package has no ``py.typed``."""

import logging

from opentelemetry.sdk._logs import LoggerProvider

class LoggingHandler(logging.Handler):
    def __init__(
        self,
        level: int = ...,
        logger_provider: LoggerProvider | None = ...,
        log_code_attributes: bool = ...,
    ) -> None: ...
