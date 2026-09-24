"""Tests for advancing active batch sessions."""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipelines.rj_iplanrio__nf_agent.utils import poll
from pipelines.rj_iplanrio__nf_agent.utils.bifrost import SubmittedBatch
from pipelines.rj_iplanrio__nf_agent.utils.pdf import PdfPages
from pipelines.rj_iplanrio__nf_agent.utils.prompts import PromptSet
from pipelines.rj_iplanrio__nf_agent.utils.settings import Settings
from pipelines.rj_iplanrio__nf_agent.utils.tracking import (
    PHASE_CLASSIFICATION,
    PHASE_EXTRACTION,
    STATE_DONE,
    STATE_FAILED,
    STATE_SUBMITTED,
    JobEvent,
    SessionContext,
)

SETTINGS = Settings("bifrost-bkt", "out-bkt", "out/path", "p.d.extracao_pagina", "p.d.nf_batch_jobs")
PROMPTS = PromptSet("v8", "classifica", "v9", "extrai {classification_hint}")
CONTEXT = SessionContext("run-1", "gs://in", "auto-abc", "v8", "v9", (PdfPages("doc", 2),))
START = JobEvent("s1", PHASE_CLASSIFICATION, "cls-batch", STATE_SUBMITTED, context=CONTEXT)


def batch(status: str, output: str | None = "gs://bkt/out/x") -> SimpleNamespace:
    return SimpleNamespace(status=status, output_file_id=output, errors=None)


@pytest.fixture
def io(vertex_row):
    appended: list[JobEvent] = []
    with (
        patch.object(poll, "active_sessions") as active,
        patch.object(poll, "retrieve_batch") as retrieve,
        patch.object(poll, "read_batch_output") as read,
        patch.object(poll, "session_start", return_value=START),
        patch.object(poll, "load_prompts", return_value=PROMPTS) as load_prompts,
        patch.object(poll, "submit_jsonl", return_value=SubmittedBatch("ext-batch", "ext-file")) as submit_jsonl,
        patch.object(poll, "append_event", side_effect=lambda table, event: appended.append(event)),
        patch.object(poll, "write_ndjson", return_value="gs://out-bkt/x.ndjson") as write,
    ):
        yield SimpleNamespace(active=active, retrieve=retrieve, read=read, load_prompts=load_prompts,
                              submit_jsonl=submit_jsonl, write=write, appended=appended, row=vertex_row)


def test_waits_on_in_progress_and_tolerates_status_errors(io):
    io.active.return_value = [START, JobEvent("s2", PHASE_CLASSIFICATION, "b2", STATE_SUBMITTED)]
    io.retrieve.side_effect = [batch("in_progress"), RuntimeError("conexão")]
    summary = poll.poll_sessions(MagicMock(), SETTINGS)
    assert summary.waiting == ["s1", "s2"]
    assert io.appended == []


def test_failed_batch_marks_session_failed(io):
    io.active.return_value = [START]
    io.retrieve.return_value = batch("expired")
    summary = poll.poll_sessions(MagicMock(), SETTINGS)
    assert summary.failed == ["s1"]
    assert io.appended[0].state == STATE_FAILED


def test_failed_write_error_is_aggregated_without_blocking_other_sessions(io):
    other = JobEvent("s2", PHASE_CLASSIFICATION, "b2", STATE_SUBMITTED)
    io.active.return_value = [START, other]
    io.retrieve.side_effect = [batch("expired"), batch("in_progress")]
    with (
        patch.object(poll, "append_event", side_effect=RuntimeError("bq indisponível")),
        pytest.raises(RuntimeError, match=r"(?s)s1.*bq indisponível"),
    ):
        poll.poll_sessions(MagicMock(), SETTINGS)
    assert [call.args[1] for call in io.retrieve.call_args_list] == ["cls-batch", "b2"]


def test_classification_done_submits_extraction_from_echoed_pages(io):
    io.active.return_value = [START]
    io.retrieve.return_value = batch("completed")
    io.read.return_value = [
        io.row("doc:1", '{"categoria": "Nenhuma das Opções"}', page_b64="UDE="),
        io.row("doc:2", '{"categoria": "NFS-e"}', page_b64="UDI="),
    ]
    summary = poll.poll_sessions(MagicMock(), SETTINGS)
    assert summary.advanced == ["s1"]
    io.load_prompts.assert_called_once_with("v8", "v9")
    data = io.submit_jsonl.call_args.args[1]
    lines = [json.loads(line) for line in data.splitlines()]
    assert [line["custom_id"] for line in lines] == ["doc:2"]
    parts = lines[0]["request"]["contents"][0]["parts"]
    assert "**NFS-e**" in parts[0]["text"]
    assert parts[1]["inlineData"]["data"] == "UDI="
    event = io.appended[0]
    assert (event.phase, event.batch_id, event.state, event.row_count) == (PHASE_EXTRACTION, "ext-batch", STATE_SUBMITTED, 1)


def test_classification_without_nf_finishes_directly(io):
    io.active.return_value = [START]
    io.retrieve.return_value = batch("completed")
    io.read.return_value = [io.row("doc:1", '{"categoria": "Nenhuma das Opções"}'),
                            io.row("doc:2", '{"categoria": "Nenhuma das Opções"}')]
    summary = poll.poll_sessions(MagicMock(), SETTINGS)
    assert summary.finished == ["s1"]
    io.submit_jsonl.assert_not_called()
    rows = io.write.call_args.args[2]
    assert [row["pagina"] for row in rows] == [1, 2]
    assert io.write.call_args.args[3] == "extracao_pagina_s1"
    assert io.appended[-1].state == STATE_DONE


def test_extraction_done_reads_classification_output_from_vertex(io):
    io.active.return_value = [JobEvent("s1", PHASE_EXTRACTION, "ext-batch", STATE_SUBMITTED)]
    io.retrieve.side_effect = lambda client, batch_id: {
        "ext-batch": batch("completed", "gs://bkt/out/ext"),
        "cls-batch": batch("completed", "gs://bkt/out/cls"),
    }[batch_id]
    io.read.side_effect = lambda uri: {
        "gs://bkt/out/cls": [io.row("doc:1", '{"categoria": "Nenhuma das Opções"}'), io.row("doc:2", '{"categoria": "NFS-e"}')],
        "gs://bkt/out/ext": [io.row("doc:2", '{"notas_fiscais": [{"numero_nf": "77"}]}')],
    }[uri]
    summary = poll.poll_sessions(MagicMock(), SETTINGS)
    assert summary.finished == ["s1"]
    rows = io.write.call_args.args[2]
    assert rows[1]["numero_documento"] == "77"
    assert all(row["versao_pipeline"]["versao_processamento"] == "auto-abc" for row in rows)
    assert rows[0]["versao_pipeline"]["run_id"] == "run-1"


def test_advance_errors_are_aggregated_after_all_sessions(io):
    other = JobEvent("s2", PHASE_CLASSIFICATION, "b2", STATE_SUBMITTED)
    io.active.return_value = [START, other]
    io.retrieve.return_value = batch("completed", None)
    with pytest.raises(RuntimeError, match=r"(?s)s1.*output_file_id.*s2"):
        poll.poll_sessions(MagicMock(), SETTINGS)


def test_legacy_session_without_context(io):
    legacy_start = JobEvent("s1", PHASE_CLASSIFICATION, "cls-batch", "validating")
    io.active.return_value = [legacy_start]
    io.retrieve.return_value = batch("completed")
    io.read.return_value = [io.row("doc:1", '{"categoria": "Nenhuma das Opções"}'),
                            io.row("doc:3", '{"categoria": "Nenhuma das Opções"}')]
    with patch.object(poll, "session_start", return_value=legacy_start):
        summary = poll.poll_sessions(MagicMock(), SETTINGS)
    assert summary.finished == ["s1"]
    io.load_prompts.assert_called_with(None, None)
    rows = io.write.call_args.args[2]
    assert [row["pagina"] for row in rows] == [1, 2, 3]
    assert rows[1]["pipeline_status"] == "erro_processamento"
