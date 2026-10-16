"""Tests for advancing active batch sessions."""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipelines.rj_iplanrio__nf_agent.utils import poll
from pipelines.rj_iplanrio__nf_agent.utils.pdf import PdfPages
from pipelines.rj_iplanrio__nf_agent.utils.prompts import PromptSet
from pipelines.rj_iplanrio__nf_agent.utils.settings import Settings
from pipelines.rj_iplanrio__nf_agent.utils.tracking import (
    PHASE_CLASSIFICATION,
    PHASE_CLASSIFICATION_RETRY,
    PHASE_EXTRACTION,
    PHASE_EXTRACTION_RETRY,
    STATE_DONE,
    STATE_FAILED,
    STATE_SUBMITTED,
    JobEvent,
    SessionContext,
)
from pipelines.rj_iplanrio__nf_agent.utils.vertex import BatchState, BatchStatus, SubmittedBatch

SETTINGS = Settings("batch-bkt", "out-bkt", "out/path", "p.d.extracao_pagina", "p.d.nf_batch_jobs")
PROMPTS = PromptSet("v8", "classifica", "v9", "extrai {classification_hint}")
CONTEXT = SessionContext("run-1", "gs://in", "auto-abc", "v8", "v9", (PdfPages("doc", 2),))
START = JobEvent("s1", PHASE_CLASSIFICATION, "cls-batch", STATE_SUBMITTED, context=CONTEXT)


STATES = {
    "completed": BatchState.SUCCEEDED,
    "in_progress": BatchState.IN_PROGRESS,
    "expired": BatchState.FAILED,
}


def batch(status: str, output: str | None = "gs://bkt/out/x") -> BatchStatus:
    return BatchStatus(STATES[status], output_dir=output)


@pytest.fixture
def io(vertex_row):
    appended: list[JobEvent] = []
    client = MagicMock()
    client.submit_jsonl.return_value = SubmittedBatch("ext-batch", "ext-file")
    with (
        patch.object(poll, "active_sessions") as active,
        patch.object(poll, "session_start", return_value=START),
        patch.object(poll, "session_batches", return_value={}) as batches,
        patch.object(poll, "load_prompts", return_value=PROMPTS) as load_prompts,
        # Mirrors append_event's signature exactly since it's used as its side_effect.
        patch.object(poll, "append_event", side_effect=lambda table, event: appended.append(event)),  # noqa: ARG005
        patch.object(poll, "write_ndjson", return_value="gs://out-bkt/x.ndjson") as write,
    ):
        yield SimpleNamespace(
            client=client,
            active=active,
            batches=batches,
            retrieve=client.retrieve,
            read=client.iter_output,
            load_prompts=load_prompts,
            submit_jsonl=client.submit_jsonl,
            write=write,
            appended=appended,
            row=vertex_row,
        )


def test_waits_on_in_progress_and_tolerates_status_errors(io):
    io.active.return_value = [START, JobEvent("s2", PHASE_CLASSIFICATION, "b2", STATE_SUBMITTED)]
    io.retrieve.side_effect = [batch("in_progress"), RuntimeError("conexão")]
    summary = poll.poll_sessions(io.client, SETTINGS)
    assert summary.waiting == ["s1", "s2"]
    assert io.appended == []


def test_failed_batch_marks_session_failed(io):
    io.active.return_value = [START]
    io.retrieve.return_value = batch("expired")
    summary = poll.poll_sessions(io.client, SETTINGS)
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
        poll.poll_sessions(io.client, SETTINGS)
    assert [call.args[0] for call in io.retrieve.call_args_list] == ["cls-batch", "b2"]


def test_classification_done_submits_extraction_from_echoed_pages(io):
    io.active.return_value = [START]
    io.retrieve.return_value = batch("completed")
    io.read.return_value = [
        io.row("doc:1", '{"categoria": "Nenhuma das Opções"}', page_b64="UDE="),
        io.row("doc:2", '{"categoria": "NFS-e"}', page_b64="UDI="),
    ]
    summary = poll.poll_sessions(io.client, SETTINGS)
    assert summary.advanced == ["s1"]
    io.load_prompts.assert_called_once_with("v8", "v9")
    data = io.submit_jsonl.call_args.args[0]
    lines = [json.loads(line) for line in data.splitlines()]
    assert [line["custom_id"] for line in lines] == ["doc:2"]
    parts = lines[0]["request"]["contents"][0]["parts"]
    assert "**NFS-e**" in parts[0]["text"]
    assert parts[1]["inlineData"]["data"] == "UDI="
    event = io.appended[0]
    assert (event.phase, event.batch_id, event.state, event.row_count) == (
        PHASE_EXTRACTION,
        "ext-batch",
        STATE_SUBMITTED,
        1,
    )


def test_classification_without_nf_finishes_directly(io):
    io.active.return_value = [START]
    io.retrieve.return_value = batch("completed")
    io.read.return_value = [
        io.row("doc:1", '{"categoria": "Nenhuma das Opções"}'),
        io.row("doc:2", '{"categoria": "Nenhuma das Opções"}'),
    ]
    summary = poll.poll_sessions(io.client, SETTINGS)
    assert summary.finished == ["s1"]
    io.submit_jsonl.assert_not_called()
    rows = io.write.call_args.args[2]
    assert [row["pagina"] for row in rows] == [1, 2]
    assert io.write.call_args.args[3] == "extracao_pagina_s1"
    assert io.appended[-1].state == STATE_DONE


def test_extraction_done_reads_classification_output_from_vertex(io):
    io.active.return_value = [JobEvent("s1", PHASE_EXTRACTION, "ext-batch", STATE_SUBMITTED)]
    io.retrieve.side_effect = lambda batch_id: {
        "ext-batch": batch("completed", "gs://bkt/out/ext"),
        "cls-batch": batch("completed", "gs://bkt/out/cls"),
    }[batch_id]
    io.read.side_effect = lambda uri: {
        "gs://bkt/out/cls": [
            io.row("doc:1", '{"categoria": "Nenhuma das Opções"}'),
            io.row("doc:2", '{"categoria": "NFS-e"}'),
        ],
        "gs://bkt/out/ext": [io.row("doc:2", '{"notas_fiscais": [{"numero_nf": "77"}]}')],
    }[uri]
    summary = poll.poll_sessions(io.client, SETTINGS)
    assert summary.finished == ["s1"]
    rows = io.write.call_args.args[2]
    assert rows[1]["numero_documento"] == "77"
    assert all(row["versao_pipeline"]["versao_processamento"] == "auto-abc" for row in rows)
    assert rows[0]["versao_pipeline"]["run_id"] == "run-1"


def test_advance_errors_are_aggregated_after_all_sessions(io):
    other = JobEvent("s2", PHASE_CLASSIFICATION, "b2", STATE_SUBMITTED)
    io.active.return_value = [START, other]
    io.retrieve.return_value = batch("completed", None)
    with pytest.raises(RuntimeError, match=r"(?s)s1.*pasta de saída.*s2"):
        poll.poll_sessions(io.client, SETTINGS)


def test_legacy_session_without_context(io):
    legacy_start = JobEvent("s1", PHASE_CLASSIFICATION, "cls-batch", "validating")
    io.active.return_value = [legacy_start]
    io.retrieve.return_value = batch("completed")
    io.read.return_value = [
        io.row("doc:1", '{"categoria": "Nenhuma das Opções"}'),
        io.row("doc:3", '{"categoria": "Nenhuma das Opções"}'),
    ]
    with patch.object(poll, "session_start", return_value=legacy_start):
        summary = poll.poll_sessions(io.client, SETTINGS)
    assert summary.finished == ["s1"]
    io.load_prompts.assert_called_with(None, None)
    rows = io.write.call_args.args[2]
    assert [row["pagina"] for row in rows] == [1, 2, 3]
    assert rows[1]["pipeline_status"] == "erro_processamento"


def test_status_errors_on_every_active_session_fail_the_run_with_the_first_message(io):
    io.active.return_value = [START, JobEvent("s2", PHASE_CLASSIFICATION, "b2", STATE_SUBMITTED)]
    io.retrieve.side_effect = RuntimeError("403 virtualkey is inactive")
    with pytest.raises(RuntimeError, match=r"(?s)nenhuma das 2 sessões.*s1.*virtualkey is inactive"):
        poll.poll_sessions(io.client, SETTINGS)
    assert io.appended == []


def test_status_error_on_the_only_active_session_fails_the_run(io):
    io.active.return_value = [START]
    io.retrieve.side_effect = RuntimeError("conexão")
    with pytest.raises(RuntimeError, match="nenhuma das 1 sessões"):
        poll.poll_sessions(io.client, SETTINGS)


def test_no_active_sessions_is_not_a_status_failure(io):
    io.active.return_value = []
    summary = poll.poll_sessions(io.client, SETTINGS)
    assert summary.waiting == []
    io.retrieve.assert_not_called()


NFSE = '{"categoria": "NFS-e"}'
NONE = '{"categoria": "Nenhuma das Opções"}'
CLS_DIR, RETRY_DIR, EXT_DIR = "gs://bkt/out/cls", "gs://bkt/out/retry", "gs://bkt/out/ext"
FAILED_ROW = '{"code":13,"message":"INTERNAL"}'


def sent_ids(io) -> list[str]:
    return [json.loads(line)["custom_id"] for line in io.submit_jsonl.call_args.args[0].splitlines()]


def test_failed_classification_pages_are_resent_alone(io):
    io.active.return_value = [START]
    io.retrieve.return_value = batch("completed")
    io.read.return_value = [
        io.row("doc:1", None, status=FAILED_ROW),
        io.row("doc:2", NFSE),
        io.row("doc:3", NONE),
        io.row("doc:4", NONE),
    ]
    summary = poll.poll_sessions(io.client, SETTINGS)
    assert summary.advanced == ["s1"]
    assert sent_ids(io) == ["doc:1"]
    event = io.appended[0]
    assert (event.phase, event.state, event.row_count) == (PHASE_CLASSIFICATION_RETRY, STATE_SUBMITTED, 1)
    io.write.assert_not_called()


def test_classification_retry_results_replace_the_failed_pages(io):
    io.active.return_value = [JobEvent("s1", PHASE_CLASSIFICATION_RETRY, "retry-batch", STATE_SUBMITTED)]
    io.retrieve.side_effect = lambda batch_id: {
        "retry-batch": batch("completed", RETRY_DIR),
        "cls-batch": batch("completed", CLS_DIR),
    }[batch_id]
    io.read.side_effect = lambda uri: {
        CLS_DIR: [io.row("doc:1", None, status=FAILED_ROW), io.row("doc:2", NFSE)],
        RETRY_DIR: [io.row("doc:1", NFSE)],
    }[uri]
    summary = poll.poll_sessions(io.client, SETTINGS)
    assert summary.advanced == ["s1"]
    assert sorted(sent_ids(io)) == ["doc:1", "doc:2"]
    assert io.appended[0].phase == PHASE_EXTRACTION


def test_a_failed_retry_job_does_not_fail_the_session(io):
    io.active.return_value = [JobEvent("s1", PHASE_CLASSIFICATION_RETRY, "retry-batch", STATE_SUBMITTED)]
    io.retrieve.side_effect = lambda batch_id: {
        "retry-batch": batch("expired", None),
        "cls-batch": batch("completed", CLS_DIR),
    }[batch_id]
    io.read.return_value = [
        io.row("doc:1", None, status=FAILED_ROW),
        io.row("doc:2", NFSE),
        io.row("doc:3", NONE),
        io.row("doc:4", NONE),
    ]
    summary = poll.poll_sessions(io.client, SETTINGS)
    assert summary.advanced == ["s1"]
    assert sent_ids(io) == ["doc:2"]
    assert all(event.state != STATE_FAILED for event in io.appended)


def test_session_fails_without_writing_results_when_most_pages_still_fail(io):
    io.active.return_value = [JobEvent("s1", PHASE_CLASSIFICATION_RETRY, "retry-batch", STATE_SUBMITTED)]
    io.retrieve.side_effect = lambda batch_id: {
        "retry-batch": batch("completed", RETRY_DIR),
        "cls-batch": batch("completed", CLS_DIR),
    }[batch_id]
    failing = [io.row(f"doc:{n}", None, status=FAILED_ROW) for n in range(1, 5)]
    io.read.side_effect = lambda uri: {CLS_DIR: failing, RETRY_DIR: failing}[uri]
    summary = poll.poll_sessions(io.client, SETTINGS)
    assert summary.failed == ["s1"]
    assert io.appended[0].state == STATE_FAILED
    assert "4 de 4" in io.appended[0].error
    io.write.assert_not_called()
    io.submit_jsonl.assert_not_called()


def test_failed_extraction_pages_are_resent_alone(io):
    io.active.return_value = [JobEvent("s1", PHASE_EXTRACTION, "ext-batch", STATE_SUBMITTED)]
    io.retrieve.side_effect = lambda batch_id: {
        "ext-batch": batch("completed", EXT_DIR),
        "cls-batch": batch("completed", CLS_DIR),
    }[batch_id]
    ok = '{"notas_fiscais": [{"numero_nf": "77"}]}'
    io.read.side_effect = lambda uri: {
        CLS_DIR: [io.row("doc:1", NFSE), io.row("doc:2", NFSE)],
        EXT_DIR: [io.row("doc:1", ok), io.row("doc:2", None, status=FAILED_ROW)],
    }[uri]
    summary = poll.poll_sessions(io.client, SETTINGS)
    assert summary.advanced == ["s1"]
    assert sent_ids(io) == ["doc:2"]
    assert io.appended[0].phase == PHASE_EXTRACTION_RETRY
    io.write.assert_not_called()


def test_extraction_retry_results_are_merged_into_the_session_output(io):
    io.active.return_value = [JobEvent("s1", PHASE_EXTRACTION_RETRY, "ret-batch", STATE_SUBMITTED)]
    io.batches.return_value = {PHASE_EXTRACTION: "ext-batch"}
    io.retrieve.side_effect = lambda batch_id: {
        "ret-batch": batch("completed", RETRY_DIR),
        "ext-batch": batch("completed", EXT_DIR),
        "cls-batch": batch("completed", CLS_DIR),
    }[batch_id]
    first = '{"notas_fiscais": [{"numero_nf": "11"}]}'
    second = '{"notas_fiscais": [{"numero_nf": "22"}]}'
    io.read.side_effect = lambda uri: {
        CLS_DIR: [io.row("doc:1", NFSE), io.row("doc:2", NFSE)],
        EXT_DIR: [io.row("doc:1", first), io.row("doc:2", None, status=FAILED_ROW)],
        RETRY_DIR: [io.row("doc:2", second)],
    }[uri]
    summary = poll.poll_sessions(io.client, SETTINGS)
    assert summary.finished == ["s1"]
    rows = io.write.call_args.args[2]
    assert [row["numero_documento"] for row in rows] == ["11", "22"]
    assert all(row["pipeline_erro"] in (None, "") for row in rows)
    assert io.appended[-1].state == STATE_DONE
    io.submit_jsonl.assert_not_called()
