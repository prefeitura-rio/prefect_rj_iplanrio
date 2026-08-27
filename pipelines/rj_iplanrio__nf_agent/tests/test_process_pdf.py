"""Regression/orchestration tests for ``POCProcessorProcessMixin.process_pdf``.

Construction strategy (documented per the task brief, see PR-1 test plan):
we build a bare ``POCProcessor`` via ``POCProcessor.__new__(POCProcessor)``
and set only the attributes/methods ``process_pdf`` actually touches, rather
than calling the real ``__init__`` (which would need real GCS/DB/Gemini
credentials via ``POCProcessorSetupMixin.__init__``). This keeps each test
focused on ``process_pdf``'s own branching logic — which is the point of
this PR — instead of also exercising cache.py/setup.py plumbing that has its
own future PR. Every ``self.*`` collaborator method
(``check_classification_cache``, ``load_all_cached_classifications``,
``check_extraction_cache``, ``preprocess_classification_page``,
``classify_page_from_cache``, ``preprocess_extraction_pdf``,
``extract_nf_from_cache``) is stubbed directly on the instance since Python
attribute lookup favors instance ``__dict__`` over the class's mixin methods.

NOTE (found while writing these tests, not fixed here per the "additive
only" PR-1 rule): ``process_pdf`` reads ``self.MAX_INTRA_PDF_WORKERS`` in its
Step 2 classification loop, but no mixin/``__init__`` in
``processing/*.py`` ever sets that attribute on a real ``POCProcessor``.
Any real call to ``process_pdf`` that reaches Step 2 would raise
``AttributeError`` today. We set it explicitly on our test doubles so these
orchestration tests aren't blocked by that latent bug — flagging it here for
the plan owner instead of silently patching production code.

``process_pdf`` no longer takes an ``ExecutionMode`` — it always runs the
full pipeline (classify → extract). The cache fast-paths (all pages already
classified, or extraction already cached) still short-circuit the loops.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from pipelines.rj_iplanrio__nf_agent.utils.processing.processor import POCProcessor


def make_processor(gcs_downloader=None, temp_dir: Path | None = None) -> POCProcessor:
    """Build a bare POCProcessor without running the real (I/O-heavy) __init__."""
    proc = POCProcessor.__new__(POCProcessor)
    proc.gcs_downloader = gcs_downloader or MagicMock()
    proc.temp_dir = temp_dir or Path()
    proc.MAX_INTRA_PDF_WORKERS = 2  # see module docstring: never set in production code today
    return proc


def extracted_nf(cnpj="11.222.333/0001-44", numero="123", valor=100.0, tipo="NFS-e", pagina=1, **extra):
    return {
        "cnpj_emitente": cnpj,
        "numero_nf": numero,
        "valor_total": valor,
        "tipo_documento": tipo,
        "pagina": pagina,
        **extra,
    }


class TestDownloadBranch:
    def test_download_failure_returns_error_result_without_raising(self, fake_gcs_downloader, tmp_path):
        fake_gcs_downloader.download_pdf_by_name.side_effect = RuntimeError("GCS unreachable")
        proc = make_processor(gcs_downloader=fake_gcs_downloader, temp_dir=tmp_path)

        result = proc.process_pdf("missing.pdf", pdf_path=None)

        assert result["success"] is False
        assert result["pdf_name"] == "missing.pdf"
        assert "Download failed" in result["error"]
        assert "GCS unreachable" in result["error"]
        # Never got far enough to attempt cleanup of a (nonexistent) local file.
        fake_gcs_downloader.cleanup_local_file.assert_not_called()

    def test_downloaded_pdf_is_cleaned_up_after_processing(self, fake_gcs_downloader, make_pdf, tmp_path):
        pdf_path = make_pdf(n_pages=1)
        fake_gcs_downloader.download_pdf_by_name.return_value = pdf_path
        proc = make_processor(gcs_downloader=fake_gcs_downloader, temp_dir=tmp_path)
        proc.check_classification_cache = MagicMock(return_value=False)
        proc.check_extraction_cache = MagicMock(return_value=(None, None))
        proc.preprocess_classification_page = MagicMock(return_value=(1, True))
        # "Outro" -> no NF pages -> Steps 3/4 skip themselves, no further stubs needed.
        proc.classify_page_from_cache = MagicMock(return_value=("Outro", "", False, None, None))

        result = proc.process_pdf("some.pdf", pdf_path=None)

        assert result["success"] is True
        fake_gcs_downloader.download_pdf_by_name.assert_called_once()
        fake_gcs_downloader.cleanup_local_file.assert_called_once_with(pdf_path)

    def test_pre_downloaded_pdf_skips_gcs_and_cleanup(self, fake_gcs_downloader, make_pdf, tmp_path):
        pdf_path = make_pdf(n_pages=1)
        proc = make_processor(gcs_downloader=fake_gcs_downloader, temp_dir=tmp_path)
        proc.check_classification_cache = MagicMock(return_value=False)
        proc.check_extraction_cache = MagicMock(return_value=(None, None))
        proc.preprocess_classification_page = MagicMock(return_value=(1, True))
        proc.classify_page_from_cache = MagicMock(return_value=("Outro", "", False, None, None))

        result = proc.process_pdf("some.pdf", pdf_path=pdf_path)

        assert result["success"] is True
        fake_gcs_downloader.download_pdf_by_name.assert_not_called()
        fake_gcs_downloader.cleanup_local_file.assert_not_called()


class TestClassificationFastPath:
    def test_extraction_cache_hit_returns_cached_extraction(self, make_pdf, tmp_path):
        pdf_path = make_pdf(n_pages=1, name="cached.pdf")
        proc = make_processor(temp_dir=tmp_path)
        proc.check_classification_cache = MagicMock(return_value=True)
        proc.load_all_cached_classifications = MagicMock(return_value=({1: "NFS-e"}, {1: "justificativa"}))
        proc.check_extraction_cache = MagicMock(
            return_value=(
                {"quantidade_notas_fiscais": 1, "notas_fiscais": [extracted_nf()]},
                [1],
            )
        )

        result = proc.process_pdf("cached.pdf", pdf_path=pdf_path)

        assert result["success"] is True
        assert result["fast_path"] is True
        assert result["nf_pages"] == [1]
        assert result["extracted_nf_count"] == 1
        assert result["extracted_nfs"] == [extracted_nf()]

    def test_all_classified_no_nf_pages_returns_empty_extraction(self, make_pdf, tmp_path):
        pdf_path = make_pdf(n_pages=1, name="no_nf.pdf")
        proc = make_processor(temp_dir=tmp_path)
        proc.check_classification_cache = MagicMock(return_value=True)
        proc.load_all_cached_classifications = MagicMock(return_value=({1: "Outro"}, {1: ""}))

        result = proc.process_pdf("no_nf.pdf", pdf_path=pdf_path)

        assert result["success"] is True
        assert result["nf_pages"] == []
        assert result["extracted_nfs"] == []


class TestFullSlowPathEndToEnd:
    def test_runs_all_steps_and_returns_extraction(self, make_pdf, tmp_path):
        pdf_path = make_pdf(n_pages=2, name="full.pdf")
        proc = make_processor(temp_dir=tmp_path)
        proc.check_classification_cache = MagicMock(return_value=False)
        proc.check_extraction_cache = MagicMock(return_value=(None, None))
        proc.preprocess_classification_page = MagicMock(side_effect=[(1, True), (2, True)])

        def classify(pdf_path_arg, page_number, skip_api_call):
            category = "NFS-e" if page_number == 1 else "Outro"
            return (category, f"just{page_number}", False, None, None)

        proc.classify_page_from_cache = MagicMock(side_effect=classify)
        proc.preprocess_extraction_pdf = MagicMock(return_value=(10, True))
        proc.extract_nf_from_cache = MagicMock(
            return_value=(
                {"quantidade_notas_fiscais": 1, "notas_fiscais": [extracted_nf(pagina=1)]},
                False,
            )
        )

        result = proc.process_pdf("full.pdf", pdf_path=pdf_path)

        assert result["success"] is True
        assert result["total_pages"] == 2
        assert result["nf_pages"] == [1]
        assert result["extracted_nf_count"] == 1
        assert result["extracted_nfs"] == [extracted_nf(pagina=1)]
        proc.preprocess_extraction_pdf.assert_called_once_with(pdf_path, [1])
        proc.extract_nf_from_cache.assert_called_once()

    def test_skips_extraction_preprocessing_when_no_nf_pages(self, make_pdf, tmp_path):
        pdf_path = make_pdf(n_pages=1, name="slow.pdf")
        proc = make_processor(temp_dir=tmp_path)
        proc.check_classification_cache = MagicMock(return_value=False)
        proc.check_extraction_cache = MagicMock(return_value=(None, None))
        proc.preprocess_classification_page = MagicMock(return_value=(1, True))
        proc.classify_page_from_cache = MagicMock(return_value=("Outro", "", False, None, None))
        proc.preprocess_extraction_pdf = MagicMock()

        result = proc.process_pdf("slow.pdf", pdf_path=pdf_path)

        assert result["success"] is True
        assert result["nf_pages"] == []
        assert result["extracted_nfs"] == []
        proc.preprocess_extraction_pdf.assert_not_called()


class TestExceptionSurfacesPartialState:
    def test_classification_error_returns_partial_state_not_raise(self, make_pdf, tmp_path):
        pdf_path = make_pdf(n_pages=3, name="broken.pdf")
        proc = make_processor(temp_dir=tmp_path)
        # Single inner worker: Step 2's ThreadPoolExecutor then processes pages
        # strictly in submission order, so page 1 is guaranteed to complete
        # before page 2 raises (with >1 worker, completion order — and thus how
        # much partial state accumulates — is a race, which would make this
        # test flaky).
        proc.MAX_INTRA_PDF_WORKERS = 1
        proc.check_classification_cache = MagicMock(return_value=False)
        proc.check_extraction_cache = MagicMock(return_value=(None, None))
        proc.preprocess_classification_page = MagicMock(return_value=(1, True))

        def classify(pdf_path_arg, page_number, skip_api_call):
            if page_number == 2:
                raise RuntimeError("Gemini classification API call failed")
            return ("Outro", "", False, None, None)

        proc.classify_page_from_cache = MagicMock(side_effect=classify)

        result = proc.process_pdf("broken.pdf", pdf_path=pdf_path)

        assert result["success"] is False
        assert "Gemini classification API call failed" in result["error"]
        assert result["total_pages"] == 3
        # Partial page_categories: at least one page succeeded before the raise.
        assert isinstance(result["page_categories"], dict)
        assert len(result["page_categories"]) >= 1
        assert all(v == "Outro" for v in result["page_categories"].values())
        assert result["extracted_nfs"] == []

    def test_extraction_error_returns_partial_state_with_nf_pages(self, make_pdf, tmp_path):
        pdf_path = make_pdf(n_pages=1, name="broken2.pdf")
        proc = make_processor(temp_dir=tmp_path)
        proc.check_classification_cache = MagicMock(return_value=False)
        proc.check_extraction_cache = MagicMock(return_value=(None, None))
        proc.preprocess_classification_page = MagicMock(return_value=(1, True))
        proc.classify_page_from_cache = MagicMock(return_value=("NFS-e", "just", False, None, None))
        proc.preprocess_extraction_pdf = MagicMock(return_value=(10, True))
        proc.extract_nf_from_cache = MagicMock(side_effect=RuntimeError("Gemini extraction API call failed"))

        result = proc.process_pdf("broken2.pdf", pdf_path=pdf_path)

        assert result["success"] is False
        assert "Gemini extraction API call failed" in result["error"]
        assert result["nf_pages"] == [1]
        assert result["page_categories"] == {1: "NFS-e"}
        assert result["extracted_nfs"] == []
