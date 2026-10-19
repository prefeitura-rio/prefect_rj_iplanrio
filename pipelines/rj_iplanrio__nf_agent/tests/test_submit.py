"""Tests for planning and submitting classification sessions."""

import json
from dataclasses import replace
from unittest.mock import MagicMock, patch

import pytest

from pipelines.rj_iplanrio__nf_agent import constants
from pipelines.rj_iplanrio__nf_agent.utils import submit
from pipelines.rj_iplanrio__nf_agent.utils.prompts import PromptSet
from pipelines.rj_iplanrio__nf_agent.utils.settings import Settings
from pipelines.rj_iplanrio__nf_agent.utils.storage import PdfRef
from pipelines.rj_iplanrio__nf_agent.utils.tracking import PHASE_CLASSIFICATION, STATE_SUBMITTED
from pipelines.rj_iplanrio__nf_agent.utils.vertex import SubmittedBatch

SETTINGS = Settings("batch-bkt", "out-bkt", "out/path", "p.d.extracao_pagina", "p.d.nf_batch_jobs")
PROMPTS = PromptSet("v1", "classifica", "v1", "extrai {classification_hint}")


def refs(*names: str) -> list[PdfRef]:
    return [PdfRef(name, f"gs://in/{name}") for name in names]


VIEW = "p.d.vw_files_pdfs_download"


def test_resolve_origem_explicit_wins_over_mes_envio():
    assert submit.resolve_origem("gs://custom/path", "2021-11-01", SETTINGS) == "gs://custom/path"


def test_resolve_origem_reads_the_month_from_the_download_view():
    with_view = replace(SETTINGS, pdfs_view_table=VIEW)
    assert submit.resolve_origem(None, "2021-11-01", with_view) == f"view://{VIEW}?mes_envio=2021-11-01"


def test_resolve_origem_rejects_bad_mes_envio_format():
    with pytest.raises(ValueError, match="mes_envio"):
        submit.resolve_origem(None, "01-11-2021", SETTINGS)


def test_resolve_origem_without_origem_or_month_raises():
    with pytest.raises(ValueError, match="mes_envio"):
        submit.resolve_origem(None, None, SETTINGS)


@pytest.fixture
def env(pdf_bytes):
    """Patches every I/O boundary; each PDF has as many pages as its name's digit."""
    batches = iter(SubmittedBatch(f"batch-{i}", f"file-{i}") for i in range(100))
    client = MagicMock()
    # Só entrega o próximo batch falso; não inspeciona os argumentos.
    client.submit_jsonl.side_effect = lambda data, name: next(batches)  # noqa: ARG005
    client.page_uri.side_effect = lambda name, number: f"gs://pages/{name}/{number:04d}.pdf"
    with (
        patch.object(submit, "load_prompts", return_value=PROMPTS),
        patch.object(submit, "list_pdfs") as list_pdfs,
        patch.object(submit, "list_downloaded_pdfs") as list_downloaded,
        patch.object(submit, "download_bytes", side_effect=lambda uri: pdf_bytes(int(uri[-1]))),
        patch.object(submit, "find_done_pdfs", return_value=set()) as done,
        patch.object(submit, "in_flight_pdf_names", return_value=set()) as in_flight,
        patch.object(submit, "append_event") as append_event,
        patch.object(submit, "active_sessions", return_value=[]) as active,
    ):
        yield {
            "client": client,
            "list_pdfs": list_pdfs,
            "list_downloaded": list_downloaded,
            "done": done,
            "in_flight": in_flight,
            "submit_jsonl": client.submit_jsonl,
            "append_event": append_event,
            "active": active,
        }


def test_requires_origem():
    with pytest.raises(ValueError, match="origem"):
        submit.submit_pending(MagicMock(), SETTINGS, submit.SubmitRequest(input_uri=None))


def test_skips_done_and_in_flight_and_records_context(env):
    env["list_pdfs"].return_value = refs("a2", "b1", "c3")
    env["done"].return_value = {"b1"}
    env["in_flight"].return_value = {"c3"}
    summary = submit.submit_pending(env["client"], SETTINGS, submit.SubmitRequest("gs://in"))
    assert summary.pdf_count == 1
    assert summary.page_count == 2  # noqa: PLR2004 -- "a2" has 2 pages per the env fixture's naming convention
    assert len(summary.session_ids) == 1
    event = env["append_event"].call_args.args[1]
    assert event.phase == PHASE_CLASSIFICATION
    assert event.state == STATE_SUBMITTED
    assert event.batch_id == "batch-0"
    assert event.row_count == 2  # noqa: PLR2004 -- "a2" has 2 pages per the env fixture's naming convention
    assert [(p.name, p.pages) for p in event.context.pdfs] == [("a2", 2)]
    assert event.context.processing_version == summary.processing_version
    assert event.context.input_uri == "gs://in"


def test_pdf_larger_than_a_batch_is_skipped(env, monkeypatch):
    env["list_pdfs"].return_value = refs("a1", "b1", "c1")
    monkeypatch.setattr(constants, "BATCH_MAX_BYTES", 1)
    summary = submit.submit_pending(env["client"], SETTINGS, submit.SubmitRequest("gs://in"))
    assert summary.session_ids == []
    assert summary.skipped == ["a1", "b1", "c1"]


def test_groups_pdfs_until_the_budget_fills(env, monkeypatch):
    env["list_pdfs"].return_value = refs("a1", "b1", "c1")
    submit.submit_pending(env["client"], SETTINGS, submit.SubmitRequest("gs://in"))
    line_bytes = len(env["submit_jsonl"].call_args.args[0]) // 3  # three identical one-page PDFs
    cost_per_pdf = line_bytes + submit.extraction_overhead_per_row(PROMPTS)
    env["submit_jsonl"].reset_mock()
    monkeypatch.setattr(constants, "BATCH_MAX_BYTES", 2 * cost_per_pdf)
    summary = submit.submit_pending(env["client"], SETTINGS, submit.SubmitRequest("gs://in"))
    assert len(summary.session_ids) == 2  # noqa: PLR2004 -- budget fits 2 of the 3 PDFs per session
    assert [call.args[0].count(b"\n") for call in env["submit_jsonl"].call_args_list] == [2, 1]
    sizes = [len(call.args[0]) for call in env["submit_jsonl"].call_args_list]
    assert all(size <= constants.BATCH_MAX_BYTES for size in sizes)


def test_max_pages_stops_before_exceeding(env):
    env["list_pdfs"].return_value = refs("a2", "b2", "c1")
    summary = submit.submit_pending(env["client"], SETTINGS, submit.SubmitRequest("gs://in", max_pages=3))
    assert summary.page_count == 2  # noqa: PLR2004 -- "a2" has 2 pages per the env fixture's naming convention
    assert summary.pdf_count == 1


def test_unreadable_pdf_is_skipped(env):
    env["list_pdfs"].return_value = refs("a1", "b1")
    with (
        patch.object(submit, "download_bytes", side_effect=[b"lixo", b"%PDF"]),
        patch.object(submit, "split_pdf_page_bytes", side_effect=[ValueError("PDF ilegível"), [b"%PDF"]]),
    ):
        summary = submit.submit_pending(env["client"], SETTINGS, submit.SubmitRequest("gs://in"))
    assert summary.skipped == ["a1"]
    assert summary.pdf_count == 1


def test_download_failure_on_one_pdf_does_not_abort_the_rest(env, pdf_bytes):
    env["list_pdfs"].return_value = refs("a1", "b1", "c1")

    def flaky_download(uri):
        if uri.endswith("b1"):
            raise TimeoutError("conexão perdida com o GCS")
        return pdf_bytes(int(uri[-1]))

    with patch.object(submit, "download_bytes", side_effect=flaky_download):
        summary = submit.submit_pending(env["client"], SETTINGS, submit.SubmitRequest("gs://in"))
    assert summary.skipped == ["b1"]
    assert summary.pdf_count == 2  # noqa: PLR2004 -- a1 e c1 têm 1 página cada, per a convenção de nomes do fixture


def test_lines_reference_pages_by_uri_and_pages_are_uploaded(env):
    env["list_pdfs"].return_value = refs("a2")
    submit.submit_pending(env["client"], SETTINGS, submit.SubmitRequest("gs://in"))
    data = env["submit_jsonl"].call_args.args[0]
    rows = [json.loads(line) for line in data.splitlines()]
    assert [row["custom_id"] for row in rows] == ["a2:1", "a2:2"]
    uris = [row["request"]["contents"][0]["parts"][1]["fileData"]["fileUri"] for row in rows]
    assert uris == ["gs://pages/a2/0001.pdf", "gs://pages/a2/0002.pdf"]
    env["client"].upload_pages.assert_called_once()
    name, pages = env["client"].upload_pages.call_args.args
    assert (name, len(pages)) == ("a2", 2)


def test_failed_upload_skips_only_that_pdf(env):
    env["list_pdfs"].return_value = refs("a1", "b1")
    env["client"].upload_pages.side_effect = [OSError("sem rede"), None]
    summary = submit.submit_pending(env["client"], SETTINGS, submit.SubmitRequest("gs://in"))
    assert summary.skipped == ["a1"]
    assert summary.pdf_count == 1


def test_stops_at_the_active_session_ceiling_and_leaves_the_rest_pending(env, monkeypatch):
    env["list_pdfs"].return_value = refs("a1", "b1", "c1", "d1")
    monkeypatch.setattr(constants, "BATCH_MAX_REQUESTS", 1)  # uma página por sessão
    settings = replace(SETTINGS, max_active_sessions=3)
    env["active"].return_value = [object()]  # já há 1 ativa: sobram 2 vagas
    summary = submit.submit_pending(env["client"], settings, submit.SubmitRequest("gs://in"))
    assert len(summary.session_ids) == 2  # noqa: PLR2004 -- 3 de teto menos 1 ativa
    assert summary.pdf_count == 2  # noqa: PLR2004
    assert summary.remaining == 2  # noqa: PLR2004


def test_submits_nothing_when_the_ceiling_is_already_reached(env):
    env["list_pdfs"].return_value = refs("a1", "b1")
    env["active"].return_value = [object()] * SETTINGS.max_active_sessions
    summary = submit.submit_pending(env["client"], SETTINGS, submit.SubmitRequest("gs://in"))
    assert (summary.session_ids, summary.remaining) == ([], 2)
    env["submit_jsonl"].assert_not_called()
    env["client"].upload_pages.assert_not_called()


def test_request_cap_closes_the_session_before_the_byte_cap(env, monkeypatch):
    env["list_pdfs"].return_value = refs("a2", "b2", "c2")
    monkeypatch.setattr(constants, "BATCH_MAX_REQUESTS", 4)
    summary = submit.submit_pending(env["client"], SETTINGS, submit.SubmitRequest("gs://in"))
    assert len(summary.session_ids) == 2  # noqa: PLR2004 -- 4 páginas na primeira, 2 na segunda
    assert [call.args[0].count(b"\n") for call in env["submit_jsonl"].call_args_list] == [4, 2]


def test_pdf_with_more_pages_than_the_request_cap_is_skipped(env, monkeypatch):
    env["list_pdfs"].return_value = refs("a3", "b1")
    monkeypatch.setattr(constants, "BATCH_MAX_REQUESTS", 2)
    summary = submit.submit_pending(env["client"], SETTINGS, submit.SubmitRequest("gs://in"))
    assert summary.skipped == ["a3"]
    assert summary.pdf_count == 1


def test_session_id_is_deterministic_for_the_same_run_and_pdfs():
    pdfs = [submit.PdfPages("a", 2), submit.PdfPages("b", 1)]
    assert submit.session_id_for("run-1", pdfs) == submit.session_id_for("run-1", pdfs)
    assert submit.session_id_for("run-1", pdfs) != submit.session_id_for("run-2", pdfs)
    assert submit.session_id_for("run-1", pdfs) != submit.session_id_for("run-1", pdfs[:1])


def test_retried_submission_with_the_same_run_id_reuses_the_session_id(env):
    env["list_pdfs"].return_value = refs("a1")
    request = submit.SubmitRequest("gs://in", run_id="task-run-1")
    first = submit.submit_pending(env["client"], SETTINGS, request)
    second = submit.submit_pending(env["client"], SETTINGS, request)
    assert first.session_ids == second.session_ids


def test_view_origin_reads_downloaded_pdfs_wherever_they_are_and_not_the_folder(env):
    env["list_downloaded"].return_value = [
        PdfRef("a1", "gs://in/raiz/a1"),
        PdfRef("b1", "gs://in/mes_envio=2022-04-01/b1"),
    ]
    origin = f"view://{SETTINGS.pdfs_view_table}?mes_envio=2022-04-01"
    summary = submit.submit_pending(env["client"], SETTINGS, submit.SubmitRequest(origin))
    env["list_downloaded"].assert_called_once_with(SETTINGS.pdfs_view_table, "2022-04-01")
    env["list_pdfs"].assert_not_called()
    assert summary.pdf_count == 2  # noqa: PLR2004 -- a1 e b1 têm 1 página cada


def test_explicit_gs_origin_still_lists_the_folder(env):
    env["list_pdfs"].return_value = refs("a1")
    submit.submit_pending(env["client"], SETTINGS, submit.SubmitRequest("gs://in"))
    env["list_pdfs"].assert_called_once_with("gs://in")
    env["list_downloaded"].assert_not_called()


def test_pdf_with_a_name_too_long_for_custom_id_is_skipped_without_aborting(env):
    long_name = "x" * 100
    env["list_pdfs"].return_value = [PdfRef(long_name, f"gs://in/{long_name}1"), PdfRef("b1", "gs://in/b1")]
    summary = submit.submit_pending(env["client"], SETTINGS, submit.SubmitRequest("gs://in"))
    assert summary.skipped == [long_name]
    assert summary.pdf_count == 1
