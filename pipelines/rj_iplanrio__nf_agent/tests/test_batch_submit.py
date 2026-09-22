"""Tests for ``utils/batch/classification_submit.py`` / ``extraction_submit.py``.

External boundaries (PDF page rendering stays real — it's pure/local; the
Bifrost ``openai.OpenAI`` client and ``append_job_event`` are monkeypatched)
— these tests exercise row-building and orchestration logic only, mirroring
``tests/test_extraction_api.py``'s approach of substituting just enough of
the real object graph to avoid network calls.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from pipelines.rj_iplanrio__nf_agent.utils.batch import classification_submit, extraction_submit
from pipelines.rj_iplanrio__nf_agent.utils.batch.custom_id import decode_custom_id
from pipelines.rj_iplanrio__nf_agent.utils.batch.extraction_submit import ExtractionCandidate
from pipelines.rj_iplanrio__nf_agent.utils.batch.row_counting import BatchSessionSelection


class TestBuildClassificationRows:
    def test_one_row_per_page_across_all_selected_pdfs(self, make_pdf):
        pdf_paths = {
            "a": make_pdf(n_pages=2, name="a.pdf"),
            "b": make_pdf(n_pages=1, name="b.pdf"),
        }
        selection = BatchSessionSelection(
            selected_pdf_names=["a", "b"], total_pages=3, skipped_pdf_names=[], unreadable_pdf_names=[]
        )

        rows = classification_submit.build_classification_rows(pdf_paths, selection, session_id="sess-1")

        assert len(rows) == 3
        identities = {
            (decode_custom_id(r["custom_id"]).pdf_name, decode_custom_id(r["custom_id"]).page_number) for r in rows
        }
        assert identities == {("a", 1), ("a", 2), ("b", 1)}
        for row in rows:
            identity = decode_custom_id(row["custom_id"])
            assert identity.session_id == "sess-1"
            assert identity.phase == "classification"
            # Vertex-native row shape — no OpenAI-style method/url/body;
            # custom_id rides along as an extra, echoed back on output rows.
            assert "method" not in row and "url" not in row and "body" not in row
            request = row["request"]
            content_parts = request["contents"][0]["parts"]
            assert content_parts[0] == {"text": request["contents"][0]["parts"][0]["text"]}
            file_part = content_parts[1]["inlineData"]
            assert file_part["mimeType"] == "application/pdf"
            assert len(file_part["data"]) > 0  # base64-encoded page bytes
            assert "generationConfig" in request

    def test_only_selected_pdfs_are_included(self, make_pdf):
        pdf_paths = {
            "a": make_pdf(n_pages=1, name="a.pdf"),
            "b": make_pdf(n_pages=1, name="b.pdf"),
        }
        # "b" is a candidate PDF but wasn't selected for this session (e.g. skipped by row budget)
        selection = BatchSessionSelection(
            selected_pdf_names=["a"], total_pages=1, skipped_pdf_names=["b"], unreadable_pdf_names=[]
        )

        rows = classification_submit.build_classification_rows(pdf_paths, selection, session_id="sess-1")

        assert {decode_custom_id(r["custom_id"]).pdf_name for r in rows} == {"a"}


class TestBuildExtractionRows:
    def test_one_row_per_candidate_with_hint_injected(self, make_pdf, monkeypatch):
        monkeypatch.setattr(extraction_submit.prompts, "EXTRACTION_PROMPT", "Extract this. {classification_hint} End.")

        pdf_paths = {"a": make_pdf(n_pages=3, name="a.pdf")}
        candidates = [
            ExtractionCandidate(pdf_name="a", page_number=2, classification_hint="NFS-e"),
        ]

        rows = extraction_submit.build_extraction_rows(pdf_paths, candidates, session_id="sess-1")

        assert len(rows) == 1
        prompt_text = rows[0]["request"]["contents"][0]["parts"][0]["text"]
        assert "NFS-e" in prompt_text  # hint was substituted into the prompt
        identity = decode_custom_id(rows[0]["custom_id"])
        assert identity.pdf_name == "a"
        assert identity.page_number == 2
        assert identity.phase == "extraction"

    def test_no_hint_when_classification_hint_is_none(self, make_pdf, monkeypatch):
        monkeypatch.setattr(extraction_submit.prompts, "EXTRACTION_PROMPT", "Extract this. {classification_hint} End.")

        pdf_paths = {"a": make_pdf(n_pages=1, name="a.pdf")}
        candidates = [ExtractionCandidate(pdf_name="a", page_number=1, classification_hint=None)]

        rows = extraction_submit.build_extraction_rows(pdf_paths, candidates, session_id="sess-1")

        prompt_text = rows[0]["request"]["contents"][0]["parts"][0]["text"]
        assert "PRÉ-CLASSIFICAÇÃO" not in prompt_text


class TestSubmitClassificationJob:
    def test_submit_uploads_file_and_creates_batch(self, make_pdf, monkeypatch):
        # BIFROST_GCS_BUCKET is required for the vertex provider's
        # storage_config (see bifrost_batch.py's module docstring for why)
        # — a dedicated bucket, deliberately separate from the pipeline's
        # own GCS_BUCKET (PDFs/results).
        monkeypatch.setenv("BIFROST_GCS_BUCKET", "rj-agent-cgm-triagem-nf-bifrost")

        fake_client = MagicMock()
        fake_uploaded_file = SimpleNamespace(id="file-abc123")
        fake_client.files.create.return_value = fake_uploaded_file
        fake_batch = SimpleNamespace(id="batch_xyz789")
        fake_client.batches.create.return_value = fake_batch

        fake_append = MagicMock()
        monkeypatch.setattr(classification_submit, "append_job_event", fake_append)

        pdf_paths = {"a": make_pdf(n_pages=2, name="a.pdf")}
        selection = BatchSessionSelection(
            selected_pdf_names=["a"], total_pages=2, skipped_pdf_names=[], unreadable_pdf_names=[]
        )

        result = classification_submit.submit_classification_job(
            client=fake_client,
            nf_batch_jobs_table="proj.ds.nf_batch_jobs",
            pdf_paths=pdf_paths,
            selection=selection,
            session_id="sess-1",
        )

        assert result.bifrost_batch_id == "batch_xyz789"
        assert result.input_file_id == "file-abc123"
        assert result.row_count == 2
        fake_client.files.create.assert_called_once()
        create_file_kwargs = fake_client.files.create.call_args.kwargs
        assert create_file_kwargs["purpose"] == "batch"
        # storage_config.gcs is required for the vertex provider's file
        # upload (Vertex AI Batch Prediction's own GCS I/O requirement, not
        # a Bifrost or pipeline choice — see bifrost_batch.py) — confirmed
        # against staging on 2026-09-19.
        create_file_storage_config = create_file_kwargs["extra_body"]["storage_config"]
        assert create_file_storage_config["gcs"]["bucket"] == "rj-agent-cgm-triagem-nf-bifrost"

        fake_client.batches.create.assert_called_once()
        create_batch_kwargs = fake_client.batches.create.call_args.kwargs
        assert create_batch_kwargs["input_file_id"] == "file-abc123"
        assert create_batch_kwargs["endpoint"] == "/v1/chat/completions"
        # batches.create needs output_folder (a gs:// URI), NOT
        # storage_config — Vertex's native BatchPredictionJob API rejects
        # storage_config there; confirmed against staging on 2026-09-19
        # (see bifrost_batch.py's module docstring).
        create_batch_extra_body = create_batch_kwargs["extra_body"]
        assert create_batch_extra_body["output_folder"]["url"] == (
            "gs://rj-agent-cgm-triagem-nf-bifrost/bifrost-batch-io/output"
        )
        assert "storage_config" not in create_batch_extra_body
        # The batch-level model id must be unprefixed ("vertex/" stripped)
        # — Vertex's native API rejects the prefixed form there with
        # "Invalid Model resource name" (confirmed against staging); the
        # prefixed form is still correct inside each JSONL row's body.model.
        assert create_batch_extra_body["model"] == "gemini-3.1-flash-lite"

        fake_append.assert_called_once()
        event = fake_append.call_args.args[1]
        assert event.bifrost_batch_id == "batch_xyz789"
        assert event.input_file_id == "file-abc123"
