"""Regression tests for ``database.build_status_rows`` (Sweep 2 PR 3).

Extracted from ``process_database``'s per-declaration loop, which used to
build a 19-column row per declaration for the now-removed excel/output_table
paths. This is the minimal shape ``BigQueryWriter.upsert_status()`` actually
reads: ``id_documento``, ``pipeline_error``, ``pipeline_classification_detail``.
"""

from __future__ import annotations

import json

import pandas as pd

from pipelines.rj_iplanrio__nf_agent.utils.pipeline.database import build_status_rows


def make_task(pdf_name: str, id_documentos: list[int]) -> dict:
    return {
        "pdf_name": pdf_name,
        "group_df": pd.DataFrame({"id_documento": id_documentos}),
    }


class TestSuccessWithClassification:
    def test_one_row_per_declaration_with_classification_detail_and_no_error(self):
        task = make_task("ok.pdf", [1, 2])
        pdf_results = {
            "ok.pdf": {
                "success": True,
                "page_categories": {1: "NFS-e"},
                "page_justifications": {1: "justificativa"},
                "nf_pages": [1],
            }
        }

        rows = build_status_rows([task], pdf_results)

        assert [r["id_documento"] for r in rows] == [1, 2]
        for row in rows:
            assert row["pipeline_error"] is None
            detail = json.loads(row["pipeline_classification_detail"])
            assert detail["valid_document_pages"] == [1]

    def test_same_pdf_level_values_repeated_across_declarations(self):
        task = make_task("ok.pdf", [10, 20, 30])
        pdf_results = {
            "ok.pdf": {
                "success": True,
                "page_categories": {1: "NFS-e"},
                "page_justifications": {},
                "nf_pages": [1],
            }
        }

        rows = build_status_rows([task], pdf_results)

        assert len(rows) == 3
        assert len({r["pipeline_classification_detail"] for r in rows}) == 1


class TestProcessingFailure:
    def test_download_failure_sets_structured_pipeline_error(self):
        task = make_task("broken.pdf", [1])
        pdf_results = {
            "broken.pdf": {
                "success": False,
                "error": "Download failed: GCS unreachable",
            }
        }

        rows = build_status_rows([task], pdf_results)

        assert len(rows) == 1
        assert rows[0]["pipeline_classification_detail"] is None
        error = json.loads(rows[0]["pipeline_error"])
        assert error["stage"] == "download"
        assert error["error_type"] == "download_failed"

    def test_extraction_failure_sets_extraction_stage(self):
        task = make_task("broken2.pdf", [1])
        pdf_results = {
            "broken2.pdf": {
                "success": False,
                "error": "Gemini extraction API call failed",
            }
        }

        rows = build_status_rows([task], pdf_results)

        error = json.loads(rows[0]["pipeline_error"])
        assert error["stage"] == "extraction"
        assert error["error_type"] == "extraction_failed"

    def test_missing_pdf_result_defaults_to_success_true(self):
        # A PDF task with no matching entry in pdf_results (e.g. never
        # reached processing) must not be treated as a failure by accident.
        task = make_task("unknown.pdf", [1])

        rows = build_status_rows([task], {})

        assert rows[0]["pipeline_error"] is not None
        error = json.loads(rows[0]["pipeline_error"])
        assert error["error_type"] == "no_classification_available"


class TestSuccessWithoutClassification:
    def test_no_page_categories_marks_no_classification_available(self):
        task = make_task("no_classif.pdf", [1])
        pdf_results = {
            "no_classif.pdf": {
                "success": True,
                "page_categories": {},
                "page_justifications": {},
                "nf_pages": [],
            }
        }

        rows = build_status_rows([task], pdf_results)

        assert rows[0]["pipeline_classification_detail"] is None
        error = json.loads(rows[0]["pipeline_error"])
        assert error["stage"] == "classification"
        assert error["error_type"] == "no_classification_available"


class TestMultiplePdfs:
    def test_rows_from_different_pdfs_dont_leak_into_each_other(self):
        tasks = [
            make_task("a.pdf", [1]),
            make_task("b.pdf", [2]),
        ]
        pdf_results = {
            "a.pdf": {"success": True, "page_categories": {1: "NFS-e"}, "page_justifications": {}, "nf_pages": [1]},
            "b.pdf": {"success": False, "error": "timeout while calling extraction API"},
        }

        rows = build_status_rows(tasks, pdf_results)

        by_id = {r["id_documento"]: r for r in rows}
        assert by_id[1]["pipeline_error"] is None
        assert by_id[2]["pipeline_error"] is not None
        assert json.loads(by_id[2]["pipeline_error"])["error_type"] == "api_timeout"
