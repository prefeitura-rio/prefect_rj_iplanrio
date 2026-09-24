"""Tests for the nf_batch_jobs event log."""

import json
from unittest.mock import patch

import pytest

from pipelines.rj_iplanrio__nf_agent.utils import tracking
from pipelines.rj_iplanrio__nf_agent.utils.pdf import PdfPages

CONTEXT = tracking.SessionContext(
    run_id="run-1",
    input_uri="gs://b/p",
    processing_version="auto-abc",
    classification_prompt_version="v8",
    extraction_prompt_version="v9",
    pdfs=(PdfPages("a", 2), PdfPages("b", 1)),
)


def test_context_json_round_trip():
    assert tracking.SessionContext.from_json(CONTEXT.to_json()) == CONTEXT
    assert json.loads(CONTEXT.to_json())["pdfs"] == [{"name": "a", "pages": 2}, {"name": "b", "pages": 1}]


def test_append_event_serializes_context():
    event = tracking.JobEvent("s1", tracking.PHASE_CLASSIFICATION, "batch-1", tracking.STATE_SUBMITTED, context=CONTEXT)
    with patch.object(tracking, "insert_row") as insert:
        tracking.append_event("p.d.t", event)
    table, row = insert.call_args.args
    assert table == "p.d.t"
    assert row["bifrost_batch_id"] == "batch-1"
    assert json.loads(row["contexto"])["run_id"] == "run-1"
    assert row["created_at"].endswith("+00:00")


def test_active_sessions_maps_rows():
    rows = [{"session_id": "s1", "phase": "extraction", "bifrost_batch_id": "b", "state": "submitted"}]
    with patch.object(tracking, "run_query", return_value=rows) as query:
        events = tracking.active_sessions("p.d.t")
    assert query.call_args.args[1:3] == ("active_sessions", "p.d.t")
    assert events == [tracking.JobEvent("s1", "extraction", "b", "submitted")]


def test_session_start_parses_context_and_requires_a_row():
    row = {"session_id": "s1", "phase": "classification", "bifrost_batch_id": "b", "state": "submitted",
           "contexto": CONTEXT.to_json()}
    with patch.object(tracking, "run_query", return_value=[row]):
        assert tracking.session_start("p.d.t", "s1").context == CONTEXT
    with patch.object(tracking, "run_query", return_value=[]), pytest.raises(RuntimeError, match="s1"):
        tracking.session_start("p.d.t", "s1")


def test_in_flight_pdf_names():
    with patch.object(tracking, "run_query", return_value=[{"nome_arquivo": "a"}, {"nome_arquivo": "b"}]):
        assert tracking.in_flight_pdf_names("p.d.t") == {"a", "b"}
