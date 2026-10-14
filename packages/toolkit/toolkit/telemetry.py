"""OpenTelemetry logs, traces and metrics, sent to an OTLP collector over HTTP.

Prefect starts a span for every flow run and task run, and the toolkit starts one for
every query, statement, upload and delete. Every Prefect and toolkit log goes to the
collector, and a log inside a run has the ``trace_id`` of the run. The backend derives
durations and error rates from the spans. The counters below are the only metrics, for
the totals that a span cannot give.

Call :func:`configure_telemetry` once when the process starts. The endpoint, the headers
and the service name are settings, so they also come from a ``.env`` file.
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

from .constants import OTEL_HANDLER_NAME, PREFECT_LOGGER, SCOPE
from .settings import settings

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
    """Send logs, traces and metrics to ``OTEL_EXPORTER_OTLP_ENDPOINT``.

    It does nothing without the endpoint, and nothing when it ran before. Call it after
    ``import prefect``, because Prefect rebuilds its logging when it is imported.

    The endpoint, the headers and the service name are the ``OTEL_*`` settings.
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
