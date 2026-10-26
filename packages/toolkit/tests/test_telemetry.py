from collections.abc import Callable
from logging import INFO, LogRecord
from types import SimpleNamespace
from typing import cast

import pytest
from opentelemetry.sdk.trace import Span, TracerProvider

from toolkit.telemetry import (
    RunCounter,
    RunLogFilter,
    RunSpanProcessor,
    read_run_attributes,
)

TAGS = [
    "environment:prod",
    "severity:high",
    "code_owner:phrmendes",
    "team:data",
    "no-colon",
]
ATTRIBUTES = {
    "environment": "prod",
    "severity": "high",
    "code_owner": "phrmendes",
}


@pytest.fixture
def in_flow_run(monkeypatch) -> Callable[[list[str]], None]:
    """Run the code as if a flow run with the given tags were active."""

    def activate(tags) -> None:
        context = SimpleNamespace(flow_run=SimpleNamespace(tags=tags))
        monkeypatch.setattr(
            "toolkit.telemetry.FlowRunContext", SimpleNamespace(get=lambda: context)
        )

    return activate


@pytest.mark.parametrize(
    ("tags", "expected"),
    [
        pytest.param(TAGS, ATTRIBUTES, id="the_three_tags_and_nothing_else"),
        pytest.param(
            ["severity:low"], {"severity": "low"}, id="a_missing_tag_is_missing"
        ),
        pytest.param(
            ["code_owner:a:b"], {"code_owner": "a:b"}, id="the_value_keeps_its_colons"
        ),
        pytest.param([], {}, id="a_run_without_tags"),
    ],
)
def test_a_run_gives_its_environment_severity_and_code_owner(
    in_flow_run, tags, expected
) -> None:
    """Read the attributes of the run from the tags of the deployment."""
    in_flow_run(tags)

    assert read_run_attributes() == expected


def test_outside_a_flow_run_there_are_no_attributes(monkeypatch) -> None:
    """Return nothing when no flow run is active."""
    monkeypatch.setattr(
        "toolkit.telemetry.FlowRunContext", SimpleNamespace(get=lambda: None)
    )

    assert read_run_attributes() == {}


def test_a_log_record_carries_the_attributes_of_the_run(in_flow_run) -> None:
    """Add the attributes to the record and keep the record."""
    in_flow_run(TAGS)
    record = LogRecord("toolkit", INFO, __file__, 1, "message", None, None)

    kept = RunLogFilter().filter(record)

    assert kept is True
    assert {key: getattr(record, key) for key in ATTRIBUTES} == ATTRIBUTES


def test_a_span_carries_the_attributes_of_the_run(in_flow_run) -> None:
    """Set the attributes on a span when it starts."""
    in_flow_run(TAGS)
    span = cast("Span", TracerProvider().get_tracer("test").start_span("run"))

    RunSpanProcessor().on_start(span)

    assert dict(span.attributes or {}) == ATTRIBUTES


def test_a_counter_adds_the_attributes_of_the_run_to_its_own(
    in_flow_run, recorder
) -> None:
    """Count with the attributes that the caller gave and those of the run."""
    in_flow_run(TAGS)

    counter = RunCounter(recorder)
    counter.add(3, {"prefix": "x"})
    counter.add(1)

    assert recorder.calls == [(3, {"prefix": "x", **ATTRIBUTES}), (1, ATTRIBUTES)]
