"""The toolkit logger, and OpenTelemetry logs, traces and metrics.

Prefect starts a span for each flow run and task run. The toolkit starts one for each
query, statement, upload and delete. Counters cover only the totals that a span cannot
give. Every log, span and counter of a flow run carries the `environment`, the
`severity` and the `code_owner` of its deployment, read from the tags of the run.
"""

from collections.abc import Mapping
from logging import Filter, LogRecord, getLogger
from typing import Protocol, final, override

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
from opentelemetry.sdk.trace import Span, SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.util.re import parse_env_headers
from prefect.context import FlowRunContext
from prefect.logging import get_logger

from .constants import OTEL_HANDLER_NAME, PREFECT_LOGGER, RUN_TAGS, SCOPE
from .settings import ToolkitSettings

logger = get_logger("toolkit")
logger.setLevel(ToolkitSettings().LOG_LEVEL)

tracer = trace.get_tracer(SCOPE)
meter = metrics.get_meter(SCOPE)


def read_run_attributes() -> dict[str, str]:
    """Return the environment, the severity and the code owner of the active flow run.

    They are the tags `environment:*`, `severity:*` and `code_owner:*` of its
    deployment. Outside a flow run, or for a tag the run lacks, there is nothing.
    """
    context = FlowRunContext.get()

    if context is None or context.flow_run is None:
        return {}

    pairs = (tag.split(":", 1) for tag in context.flow_run.tags if ":" in tag)

    return {key: value for key, value in pairs if key in RUN_TAGS}


class RunLogFilter(Filter):
    """Add the attributes of the flow run to a log record."""

    @override
    def filter(self, record: LogRecord) -> bool:
        """Set each attribute on the record, and keep the record."""
        for key, value in read_run_attributes().items():
            setattr(record, key, value)

        return True


class RunSpanProcessor(SpanProcessor):
    """Add the attributes of the flow run to a span when it starts."""

    @override
    def on_start(
        self, span: Span, parent_context: object = None
    ) -> None:  # the SDK passes it
        """Set the attributes on the span."""
        span.set_attributes(read_run_attributes())


class Adder(Protocol):
    """What a counter does: add an amount with attributes."""

    def add(self, amount: int, attributes: Mapping[str, str] | None = None) -> None:
        """Add the amount."""


@final
class RunCounter:
    """A counter that adds the attributes of the flow run to those of the caller."""

    def __init__(self, counter: Adder) -> None:
        """Wrap the counter."""
        self.counter = counter

    def add(self, amount: int, attributes: Mapping[str, str] | None = None) -> None:
        """Add the amount with the attributes of the caller and of the flow run."""
        self.counter.add(amount, {**(attributes or {}), **read_run_attributes()})


QUERY_ROWS = RunCounter(
    meter.create_counter(
        "toolkit.query.rows", unit="{row}", description="Rows that a query returned."
    )
)
UPLOADED_FILES = RunCounter(
    meter.create_counter(
        "toolkit.gcs.uploaded.files", unit="{file}", description="Files uploaded."
    )
)
UPLOADED_BYTES = RunCounter(
    meter.create_counter(
        "toolkit.gcs.uploaded.bytes", unit="By", description="Bytes uploaded."
    )
)
DELETED_FILES = RunCounter(
    meter.create_counter(
        "toolkit.gcs.deleted.files", unit="{file}", description="Objects deleted."
    )
)


def configure_telemetry() -> None:
    """Send logs, traces and metrics to `OTEL_EXPORTER_OTLP_ENDPOINT`.

    It does nothing without the endpoint, or if it already ran. This module calls it
    when it is imported, which is after `prefect.logging`, so every pipeline that
    imports the toolkit or `utils` sends its telemetry.
    """
    settings = ToolkitSettings()
    endpoint = settings.OTEL_EXPORTER_OTLP_ENDPOINT.rstrip("/")
    prefect_logger = getLogger(PREFECT_LOGGER)

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
    handler.addFilter(RunLogFilter())
    prefect_logger.addHandler(handler)

    spans = TracerProvider(resource=resource)
    spans.add_span_processor(RunSpanProcessor())
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


configure_telemetry()
