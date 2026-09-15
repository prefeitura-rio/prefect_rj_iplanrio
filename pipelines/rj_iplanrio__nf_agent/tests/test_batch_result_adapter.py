"""Tests for ``utils/batch/result_adapter.py``.

These exercise pure parsing/reshaping logic against hand-built Bifrost
Batch API result lines — no Bifrost/BigQuery/GCS calls are made. Row shapes
mirror https://docs.getbifrost.ai/api-reference/batch/get-batch-results
(``custom_id`` + ``response.body`` shaped like a standard OpenAI
chat-completions response), same as OpenAI's own Batch API — see
``result_adapter.py``'s module docstring for the exact reference.
"""

from __future__ import annotations

import json

from pipelines.rj_iplanrio__nf_agent.utils.batch import result_adapter
from pipelines.rj_iplanrio__nf_agent.utils.batch.custom_id import encode_custom_id


def _response_row(
    pdf_name: str, page_number: int, text_payload: dict, phase: str = "classification", usage: dict | None = None
) -> dict:
    """Build a successful batch-result line with an embedded model text payload."""
    usage = usage or {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
    return {
        "custom_id": encode_custom_id(phase, "sess-1", pdf_name, page_number),
        "response": {
            "status_code": 200,
            "body": {
                "choices": [{"message": {"role": "assistant", "content": json.dumps(text_payload)}}],
                "usage": usage,
            },
        },
    }


def _failed_row(pdf_name: str, page_number: int, message: str, phase: str = "classification") -> dict:
    return {
        "custom_id": encode_custom_id(phase, "sess-1", pdf_name, page_number),
        "error": {"code": "provider_error", "message": message},
    }


class TestParseClassificationOutputRows:
    def test_successful_row_parses_category_and_usage(self):
        row = _response_row("doc.pdf", 1, {"categoria": "NFS-e", "justificativa": "tem CNPJ e valor"})

        results = result_adapter.parse_classification_output_rows([row])

        assert len(results) == 1
        parsed = results[0]
        assert parsed.pdf_name == "doc.pdf"
        assert parsed.page_number == 1
        assert parsed.category == "NFS-e"
        assert parsed.justification == "tem CNPJ e valor"
        assert parsed.usage == {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
        assert parsed.error is None

    def test_failed_row_has_none_category_and_error_set(self):
        row = _failed_row("doc.pdf", 2, "Bad Request: invalid role")

        results = result_adapter.parse_classification_output_rows([row])

        assert results[0].category is None
        assert results[0].error == "Bad Request: invalid role"

    def test_malformed_json_response_is_treated_as_error_not_raised(self):
        row = {
            "custom_id": encode_custom_id("classification", "sess-1", "doc.pdf", 3),
            "response": {
                "status_code": 200,
                "body": {"choices": [{"message": {"content": "not valid json {{{"}}], "usage": {}},
            },
        }

        results = result_adapter.parse_classification_output_rows([row])

        assert results[0].category is None
        assert "Failed to parse" in results[0].error

    def test_non_200_status_code_is_treated_as_error(self):
        row = {
            "custom_id": encode_custom_id("classification", "sess-1", "doc.pdf", 1),
            "response": {"status_code": 429, "body": {}},
        }

        results = result_adapter.parse_classification_output_rows([row])

        assert results[0].category is None
        assert "429" in results[0].error


class TestParseExtractionOutputRows:
    def test_successful_extraction_row(self):
        payload = {"possui_nota_fiscal": True, "quantidade_notas_fiscais": 1, "notas_fiscais": [{"numero_nf": "123"}]}
        row = _response_row("doc.pdf", 5, payload, phase="extraction")

        results = result_adapter.parse_extraction_output_rows([row])

        assert results[0].extracted["quantidade_notas_fiscais"] == 1
        assert results[0].error is None

    def test_failed_extraction_row(self):
        row = _failed_row("doc.pdf", 5, "quota exceeded", phase="extraction")

        results = result_adapter.parse_extraction_output_rows([row])

        assert results[0].extracted is None
        assert results[0].error == "quota exceeded"


class TestNfPagesFromClassification:
    def test_groups_nf_pages_by_pdf_and_sorts(self):
        rows = [
            result_adapter.ClassificationOutputRow("a.pdf", 3, "NFS-e", "", {}, None),
            result_adapter.ClassificationOutputRow("a.pdf", 1, "NF-e", "", {}, None),
            result_adapter.ClassificationOutputRow("a.pdf", 2, "Nenhuma das Opções", "", {}, None),
            result_adapter.ClassificationOutputRow("b.pdf", 1, "Fatura", "", {}, None),
        ]

        by_pdf = result_adapter.nf_pages_from_classification(rows)

        assert by_pdf == {"a.pdf": [1, 3], "b.pdf": [1]}

    def test_failed_rows_are_excluded(self):
        rows = [result_adapter.ClassificationOutputRow("a.pdf", 1, None, "", {}, "some error")]

        by_pdf = result_adapter.nf_pages_from_classification(rows)

        assert by_pdf == {}


class TestTotalPagesByPdfFromClassification:
    def test_counts_distinct_pages_per_pdf(self):
        rows = [
            result_adapter.ClassificationOutputRow("a.pdf", 1, "NF-e", "", {}, None),
            result_adapter.ClassificationOutputRow("a.pdf", 2, "Nenhuma das Opções", "", {}, None),
            result_adapter.ClassificationOutputRow("b.pdf", 1, "Fatura", "", {}, None),
        ]

        assert result_adapter.total_pages_by_pdf_from_classification(rows) == {"a.pdf": 2, "b.pdf": 1}


class TestBuildPdfResultsFromBatch:
    def test_pdf_with_no_nf_pages_has_empty_extracted_nfs(self):
        classification_rows = [
            result_adapter.ClassificationOutputRow("a.pdf", 1, "Nenhuma das Opções", "vazio", {}, None),
        ]

        pdf_results = result_adapter.build_pdf_results_from_batch(
            classification_rows, extraction_rows=[], total_pages_by_pdf={"a.pdf": 1}
        )

        result = pdf_results["a.pdf"]
        assert result["success"] is True
        assert result["nf_pages"] == []
        assert result["extracted_nfs"] == []
        assert result["page_categories"] == {1: "Nenhuma das Opções"}

    def test_pdf_with_extracted_nf_remaps_pagina_to_original_page_number(self):
        classification_rows = [
            result_adapter.ClassificationOutputRow("a.pdf", 5, "NFS-e", "tem valor", {}, None),
        ]
        extraction_rows = [
            result_adapter.ExtractionOutputRow(
                "a.pdf", 5, {"notas_fiscais": [{"numero_nf": "1", "pagina": 1}]}, {}, None
            ),
        ]

        pdf_results = result_adapter.build_pdf_results_from_batch(
            classification_rows, extraction_rows, total_pages_by_pdf={"a.pdf": 5}
        )

        result = pdf_results["a.pdf"]
        assert result["extracted_nfs"][0]["pagina"] == 5  # remapped from the model's local "1" to real page 5
        assert result["nf_pages"] == [5]

    def test_classification_error_marks_pdf_as_failed(self):
        classification_rows = [
            result_adapter.ClassificationOutputRow("a.pdf", 1, None, "", {}, "Bifrost error: timeout"),
        ]

        pdf_results = result_adapter.build_pdf_results_from_batch(
            classification_rows, extraction_rows=[], total_pages_by_pdf={"a.pdf": 1}
        )

        result = pdf_results["a.pdf"]
        assert result["success"] is False
        assert "Bifrost error: timeout" in result["error"]

    def test_pdf_absent_from_classification_rows_still_gets_an_entry(self):
        # total_pages_by_pdf is the source of truth for which PDFs exist in
        # the session — a PDF with zero classification rows (e.g. every row
        # failed) must still produce an entry so build_extracao_pagina_rows
        # can emit its per-page erro_processamento rows downstream.
        pdf_results = result_adapter.build_pdf_results_from_batch(
            classification_rows=[], extraction_rows=[], total_pages_by_pdf={"a.pdf": 3}
        )

        assert "a.pdf" in pdf_results
        assert pdf_results["a.pdf"]["total_pages"] == 3
        assert pdf_results["a.pdf"]["page_categories"] == {}
