import logging
from collections.abc import Iterator
from io import StringIO
from types import SimpleNamespace

import pytest
from opentelemetry import metrics, trace
from opentelemetry.sdk._logs.export import InMemoryLogRecordExporter
from opentelemetry.sdk.metrics.export import ConsoleMetricExporter
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pydantic import SecretStr

from toolkit import telemetry
from toolkit.constants import OTEL_HANDLER_NAME, PREFECT_LOGGER
from toolkit.settings import Settings


@pytest.fixture
def prefect_logger() -> Iterator[logging.Logger]:
    logger = logging.getLogger(PREFECT_LOGGER)
    before = list(logger.handlers)
    yield logger
    logger.handlers = before


@pytest.fixture
def collector(monkeypatch) -> SimpleNamespace:
    """Replace the OTLP exporters and the global setters, and record what is built."""
    built: dict[str, tuple[str | None, dict[str, str] | None]] = {}
    providers: list[object] = []

    def exporter(name: str, factory):
        def build(endpoint: str | None = None, headers: dict[str, str] | None = None):
            built[name] = (endpoint, headers)
            return factory()

        return build

    for name, target, factory in [
        ("logs", "OTLPLogExporter", InMemoryLogRecordExporter),
        ("traces", "OTLPSpanExporter", InMemorySpanExporter),
        (
            "metrics",
            "OTLPMetricExporter",
            lambda: ConsoleMetricExporter(out=StringIO()),
        ),
    ]:
        monkeypatch.setattr(telemetry, target, exporter(name, factory))
    monkeypatch.setattr(metrics, "set_meter_provider", providers.append)
    monkeypatch.setattr(trace, "set_tracer_provider", providers.append)
    return SimpleNamespace(built=built, providers=providers)


def configure(
    monkeypatch: pytest.MonkeyPatch, endpoint: str, headers: str = ""
) -> None:
    config = Settings(
        OTEL_EXPORTER_OTLP_ENDPOINT=endpoint,
        OTEL_EXPORTER_OTLP_HEADERS=SecretStr(headers),
    )
    monkeypatch.setattr(telemetry, "settings", config)
    telemetry.configure_telemetry()


@pytest.mark.parametrize(
    ("endpoint", "headers", "expected_headers"),
    [
        pytest.param("http://c:4318", "", None, id="no_headers"),
        pytest.param("http://c:4318/", "", None, id="a_trailing_slash_is_dropped"),
        pytest.param(
            "http://c:4318", "key=abc,x=y", {"key": "abc", "x": "y"}, id="headers"
        ),
    ],
)
def test_each_signal_goes_to_its_own_path_with_the_headers(
    collector, prefect_logger, monkeypatch, endpoint, headers, expected_headers
):
    configure(monkeypatch, endpoint, headers)

    assert collector.built == {
        "logs": ("http://c:4318/v1/logs", expected_headers),
        "traces": ("http://c:4318/v1/traces", expected_headers),
        "metrics": ("http://c:4318/v1/metrics", expected_headers),
    }
    for provider in collector.providers:
        provider.shutdown()


def test_without_an_endpoint_nothing_is_configured(
    collector, prefect_logger, monkeypatch
):
    configure(monkeypatch, "")

    assert (collector.built, collector.providers) == ({}, [])


def test_configuring_twice_adds_one_handler(collector, prefect_logger, monkeypatch):
    configure(monkeypatch, "http://c:4318")
    configure(monkeypatch, "http://c:4318")

    assert [h.get_name() for h in prefect_logger.handlers].count(OTEL_HANDLER_NAME) == 1
    for provider in collector.providers:
        provider.shutdown()
