"""The toolkit logger, and OpenTelemetry logs, traces and metrics.

Prefect starts a span for each flow run and task run. The toolkit starts one for each
query, statement, upload and delete. Counters cover only the totals that a span cannot
give.
"""

import logging

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.logging.handler import LoggingHandler
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.util.re import parse_env_headers
from prefect.logging import get_logger

from .constants import OTEL_HANDLER_NAME, PREFECT_LOGGER, SCOPE
from .settings import settings

logger = get_logger("toolkit")
logger.setLevel(settings.LOG_LEVEL)

tracer = trace.get_tracer(SCOPE)
meter = metrics.get_meter(SCOPE)

QUERY_ROWS = meter.create_counter(
    "toolkit.query.rows", unit="{row}", description="Rows that a query returned."
)
UPLOADED_FILES = meter.create_counter(
    "toolkit.gcs.uploaded.files", unit="{file}", description="Files uploaded."
)
UPLOADED_BYTES = meter.create_counter(
    "toolkit.gcs.uploaded.bytes", unit="By", description="Bytes uploaded."
)
DELETED_FILES = meter.create_counter(
    "toolkit.gcs.deleted.files", unit="{file}", description="Objects deleted."
)


def configure_telemetry() -> None:
    """Send logs, traces and metrics to `OTEL_EXPORTER_OTLP_ENDPOINT`.

    It does nothing without the endpoint, or if it already ran. Call it after `import
    prefect`, which rebuilds the logging.
    """
    endpoint = settings.OTEL_EXPORTER_OTLP_ENDPOINT.rstrip("/")
    prefect_logger = logging.getLogger(PREFECT_LOGGER)

    if not endpoint or any(
        handler.get_name() == OTEL_HANDLER_NAME for handler in prefect_logger.handlers
    ):
        return

    raw_headers = settings.OTEL_EXPORTER_OTLP_HEADERS.get_secret_value()
    headers = parse_env_headers(raw_headers) if raw_headers else None
    resource = Resource.create({"service.name": settings.OTEL_SERVICE_NAME})

    logs = LoggerProvider(resource=resource)
    logs.add_log_record_processor(
        BatchLogRecordProcessor(
            OTLPLogExporter(endpoint=f"{endpoint}/v1/logs", headers=headers)
        )
    )

    handler = LoggingHandler(logger_provider=logs)
    handler.set_name(OTEL_HANDLER_NAME)
    prefect_logger.addHandler(handler)

    spans = TracerProvider(resource=resource)
    spans.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(endpoint=f"{endpoint}/v1/traces", headers=headers)
        )
    )

    trace.set_tracer_provider(spans)

    metrics.set_meter_provider(
        MeterProvider(
            resource=resource,
            metric_readers=[
                PeriodicExportingMetricReader(
                    OTLPMetricExporter(
                        endpoint=f"{endpoint}/v1/metrics", headers=headers
                    )
                )
            ],
        )
    )
