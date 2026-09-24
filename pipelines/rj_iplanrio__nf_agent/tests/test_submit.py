"""Tests for planning and submitting classification sessions."""

from unittest.mock import MagicMock, patch

import pytest

from pipelines.rj_iplanrio__nf_agent import constants
from pipelines.rj_iplanrio__nf_agent.utils import submit
from pipelines.rj_iplanrio__nf_agent.utils.bifrost import SubmittedBatch
from pipelines.rj_iplanrio__nf_agent.utils.prompts import PromptSet
from pipelines.rj_iplanrio__nf_agent.utils.settings import Settings
from pipelines.rj_iplanrio__nf_agent.utils.storage import PdfRef
from pipelines.rj_iplanrio__nf_agent.utils.tracking import PHASE_CLASSIFICATION, STATE_SUBMITTED

SETTINGS = Settings("bifrost-bkt", "out-bkt", "out/path", "p.d.extracao_pagina", "p.d.nf_batch_jobs")
PROMPTS = PromptSet("v1", "classifica", "v1", "extrai {classification_hint}")


def refs(*names: str) -> list[PdfRef]:
    return [PdfRef(name, f"gs://in/{name}") for name in names]


@pytest.fixture
def env(pdf_bytes):
    """Patches every I/O boundary; each PDF has as many pages as its name's digit."""
    batches = iter(SubmittedBatch(f"batch-{i}", f"file-{i}") for i in range(100))
    with (
        patch.object(submit, "load_prompts", return_value=PROMPTS),
        patch.object(submit, "list_pdfs") as list_pdfs,
        patch.object(submit, "download_bytes", side_effect=lambda uri: pdf_bytes(int(uri[-1]))),
        patch.object(submit, "find_done_pdfs", return_value=set()) as done,
        patch.object(submit, "in_flight_pdf_names", return_value=set()) as in_flight,
        patch.object(submit, "submit_jsonl", side_effect=lambda *a, **k: next(batches)) as submit_jsonl,
        patch.object(submit, "append_event") as append_event,
    ):
        yield {"list_pdfs": list_pdfs, "done": done, "in_flight": in_flight,
               "submit_jsonl": submit_jsonl, "append_event": append_event}


def test_requires_origem():
    with pytest.raises(ValueError, match="origem"):
        submit.submit_pending(MagicMock(), SETTINGS, submit.SubmitRequest(input_uri=None))


def test_skips_done_and_in_flight_and_records_context(env):
    env["list_pdfs"].return_value = refs("a2", "b1", "c3")
    env["done"].return_value = {"b1"}
    env["in_flight"].return_value = {"c3"}
    summary = submit.submit_pending(MagicMock(), SETTINGS, submit.SubmitRequest("gs://in"))
    assert summary.pdf_count == 1
    assert summary.page_count == 2
    assert len(summary.session_ids) == 1
    event = env["append_event"].call_args.args[1]
    assert event.phase == PHASE_CLASSIFICATION
    assert event.state == STATE_SUBMITTED
    assert event.batch_id == "batch-0"
    assert event.row_count == 2
    assert [(p.name, p.pages) for p in event.context.pdfs] == [("a2", 2)]
    assert event.context.processing_version == summary.processing_version
    assert event.context.input_uri == "gs://in"


def test_pdf_larger_than_a_batch_is_skipped(env, monkeypatch):
    env["list_pdfs"].return_value = refs("a1", "b1", "c1")
    monkeypatch.setattr(constants, "BATCH_MAX_BYTES", 1)
    summary = submit.submit_pending(MagicMock(), SETTINGS, submit.SubmitRequest("gs://in"))
    assert summary.session_ids == []
    assert summary.skipped == ["a1", "b1", "c1"]


def test_groups_pdfs_until_the_budget_fills(env, monkeypatch):
    env["list_pdfs"].return_value = refs("a1", "b1", "c1")
    submit.submit_pending(MagicMock(), SETTINGS, submit.SubmitRequest("gs://in"))
    line_bytes = len(env["submit_jsonl"].call_args.args[1]) // 3  # three identical one-page PDFs
    cost_per_pdf = line_bytes + submit.extraction_overhead_per_row(PROMPTS)
    env["submit_jsonl"].reset_mock()
    monkeypatch.setattr(constants, "BATCH_MAX_BYTES", 2 * cost_per_pdf)
    summary = submit.submit_pending(MagicMock(), SETTINGS, submit.SubmitRequest("gs://in"))
    assert len(summary.session_ids) == 2
    assert [call.args[1].count(b"\n") for call in env["submit_jsonl"].call_args_list] == [2, 1]
    sizes = [len(call.args[1]) for call in env["submit_jsonl"].call_args_list]
    assert all(size <= constants.BATCH_MAX_BYTES for size in sizes)


def test_max_pages_stops_before_exceeding(env):
    env["list_pdfs"].return_value = refs("a2", "b2", "c1")
    summary = submit.submit_pending(MagicMock(), SETTINGS, submit.SubmitRequest("gs://in", max_pages=3))
    assert summary.page_count == 2
    assert summary.pdf_count == 1


def test_unreadable_pdf_is_skipped(env):
    env["list_pdfs"].return_value = refs("a1", "b1")
    with patch.object(submit, "download_bytes", side_effect=[b"lixo", b"%PDF"]), \
         patch.object(submit, "split_pdf_pages", side_effect=[ValueError("PDF ilegível"), ["QUJD"]]):
        summary = submit.submit_pending(MagicMock(), SETTINGS, submit.SubmitRequest("gs://in"))
    assert summary.skipped == ["a1"]
    assert summary.pdf_count == 1
