"""Tests for ``utils/batch/classification_submit.py`` / ``extraction_submit.py``.

External boundaries (GCS upload, BigQuery load/client, Vertex AI client) are
monkeypatched — these tests exercise row-building and orchestration logic
only, mirroring ``tests/test_extraction_api.py``'s approach of substituting
just enough of the real object graph to avoid network calls.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from pipelines.rj_iplanrio__nf_agent.utils.batch import classification_submit, extraction_submit
from pipelines.rj_iplanrio__nf_agent.utils.batch.extraction_submit import ExtractionCandidate
from pipelines.rj_iplanrio__nf_agent.utils.batch.row_counting import BatchSessionSelection


@pytest.fixture
def fake_gcs_downloader():
    downloader = MagicMock()
    downloader.bucket = MagicMock(name="gs://fake-bucket")
    return downloader


class TestBuildClassificationRows:
    def test_one_row_per_page_across_all_selected_pdfs(self, make_pdf, monkeypatch, fake_gcs_downloader):
        def _fake_upload(*, pdf_path, page_number, **_kwargs):
            return f"gs://bucket/{pdf_path.stem}__p{page_number}.pdf"

        monkeypatch.setattr(classification_submit, "upload_page_pdf", _fake_upload)

        pdf_paths = {
            "a": make_pdf(n_pages=2, name="a.pdf"),
            "b": make_pdf(n_pages=1, name="b.pdf"),
        }
        selection = BatchSessionSelection(
            selected_pdf_names=["a", "b"], total_pages=3, skipped_pdf_names=[], unreadable_pdf_names=[]
        )

        rows = classification_submit.build_classification_rows(
            fake_gcs_downloader, pdf_paths, selection, session_id="sess-1"
        )

        assert len(rows) == 3
        assert {(r["pdf_name"], r["page_number"]) for r in rows} == {("a", 1), ("a", 2), ("b", 1)}
        for row in rows:
            assert row["session_id"] == "sess-1"
            assert row["request"]["contents"][0]["role"] == "user"
            file_data = row["request"]["contents"][0]["parts"][1]["fileData"]
            assert file_data["mimeType"] == "application/pdf"
            assert file_data["fileUri"].startswith("gs://bucket/")

    def test_only_selected_pdfs_are_included(self, make_pdf, monkeypatch, fake_gcs_downloader):
        monkeypatch.setattr(classification_submit, "upload_page_pdf", lambda **_kwargs: "gs://bucket/x.pdf")

        pdf_paths = {
            "a": make_pdf(n_pages=1, name="a.pdf"),
            "b": make_pdf(n_pages=1, name="b.pdf"),
        }
        # "b" is a candidate PDF but wasn't selected for this session (e.g. skipped by row budget)
        selection = BatchSessionSelection(
            selected_pdf_names=["a"], total_pages=1, skipped_pdf_names=["b"], unreadable_pdf_names=[]
        )

        rows = classification_submit.build_classification_rows(
            fake_gcs_downloader, pdf_paths, selection, session_id="sess-1"
        )

        assert {r["pdf_name"] for r in rows} == {"a"}


class TestBuildExtractionRows:
    def test_one_row_per_candidate_with_hint_injected(self, make_pdf, monkeypatch, fake_gcs_downloader):
        monkeypatch.setattr(extraction_submit, "upload_page_pdf", lambda **_kwargs: "gs://bucket/page.pdf")
        monkeypatch.setattr(extraction_submit.prompts, "EXTRACTION_PROMPT", "Extract this. {classification_hint} End.")

        pdf_paths = {"a": make_pdf(n_pages=3, name="a.pdf")}
        candidates = [
            ExtractionCandidate(pdf_name="a", page_number=2, classification_hint="NFS-e"),
        ]

        rows = extraction_submit.build_extraction_rows(fake_gcs_downloader, pdf_paths, candidates, session_id="sess-1")

        assert len(rows) == 1
        prompt_text = rows[0]["request"]["contents"][0]["parts"][0]["text"]
        assert "NFS-e" in prompt_text  # hint was substituted into the prompt
        assert rows[0]["pdf_name"] == "a"
        assert rows[0]["page_number"] == 2

    def test_no_hint_when_classification_hint_is_none(self, make_pdf, monkeypatch, fake_gcs_downloader):
        monkeypatch.setattr(extraction_submit, "upload_page_pdf", lambda **_kwargs: "gs://bucket/page.pdf")
        monkeypatch.setattr(extraction_submit.prompts, "EXTRACTION_PROMPT", "Extract this. {classification_hint} End.")

        pdf_paths = {"a": make_pdf(n_pages=1, name="a.pdf")}
        candidates = [ExtractionCandidate(pdf_name="a", page_number=1, classification_hint=None)]

        rows = extraction_submit.build_extraction_rows(fake_gcs_downloader, pdf_paths, candidates, session_id="sess-1")

        prompt_text = rows[0]["request"]["contents"][0]["parts"][0]["text"]
        assert "PRÉ-CLASSIFICAÇÃO" not in prompt_text


class TestSubmitClassificationJob:
    def test_submit_loads_table_and_calls_vertex_create(self, make_pdf, monkeypatch, fake_gcs_downloader):
        monkeypatch.setattr(classification_submit, "upload_page_pdf", lambda **_kwargs: "gs://bucket/page.pdf")

        fake_bq_client = MagicMock()
        fake_load_job = MagicMock()
        fake_bq_client.load_table_from_json.return_value = fake_load_job
        monkeypatch.setattr(classification_submit.bigquery, "Client", lambda **_kwargs: fake_bq_client)

        fake_vertex_client = MagicMock()
        fake_job = SimpleNamespace(name="projects/p/locations/l/batchPredictionJobs/123", state="JOB_STATE_PENDING")
        fake_vertex_client.batches.create.return_value = fake_job
        monkeypatch.setattr(classification_submit, "build_vertex_batch_client", lambda: fake_vertex_client)

        fake_append = MagicMock()
        monkeypatch.setattr(classification_submit, "append_job_event", fake_append)

        pdf_paths = {"a": make_pdf(n_pages=2, name="a.pdf")}
        selection = BatchSessionSelection(
            selected_pdf_names=["a"], total_pages=2, skipped_pdf_names=[], unreadable_pdf_names=[]
        )

        result = classification_submit.submit_classification_job(
            bq_project="proj",
            bq_dataset="ds",
            nf_batch_jobs_table="proj.ds.nf_batch_jobs",
            gcs_downloader=fake_gcs_downloader,
            pdf_paths=pdf_paths,
            selection=selection,
            session_id="sess-1",
        )

        assert result.vertex_job_name == fake_job.name
        assert result.row_count == 2
        fake_bq_client.load_table_from_json.assert_called_once()
        fake_vertex_client.batches.create.assert_called_once()
        create_kwargs = fake_vertex_client.batches.create.call_args.kwargs
        assert create_kwargs["src"] == f"bq://{result.input_table}"
        assert create_kwargs["config"]["dest"] == f"bq://{result.output_table}"
        fake_append.assert_called_once()
