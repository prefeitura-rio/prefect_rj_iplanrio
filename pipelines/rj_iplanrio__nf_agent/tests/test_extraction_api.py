"""Regression/orchestration tests for extraction/api.py (extract_from_pdf / _extract_from_pdf_bytes).

Construction strategy: ``NFExtractor.__init__`` builds its model lazily via
``auth.get_model`` -> ``utils.llm.build_gemini_model`` (Bifrost). We bypass
``__init__`` entirely via ``NFExtractor.__new__(NFExtractor)`` and set
``self._model`` directly to a controllable fake — ``auth.get_model`` just
returns ``self._model`` when it's not None, so this is a clean substitution
with no monkeypatching of the model builder needed.

Rate limiter / metrics tracker: per the task brief, we use the real
``iplanrio_agent_toolkit`` singletons (harmless, in-memory) rather than
mocking them — see ``conftest.py::_disable_real_rate_limiter``.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pipelines.rj_iplanrio__nf_agent.utils.extraction import auth, coalesce
from pipelines.rj_iplanrio__nf_agent.utils.extraction.extractor import NFExtractor


class FakeGeminiResponse:
    """Minimal stand-in for the google.generativeai response object."""

    def __init__(self, payload: dict, usage_metadata=None, candidates=None):
        self.text = json.dumps(payload, ensure_ascii=False)
        self.usage_metadata = usage_metadata or MagicMock(
            prompt_token_count=10, candidates_token_count=10, total_token_count=20
        )
        self.candidates = candidates or []


def make_extractor(
    model_name: str = "gemini-3.1-flash-lite", batch_size: int = 5, prompt: str = "PROMPT"
) -> NFExtractor:
    """Build a bare NFExtractor without running the real (credential-requiring) __init__."""
    extractor = NFExtractor.__new__(NFExtractor)
    extractor.model_name = model_name
    extractor.extraction_prompt = prompt
    extractor.batch_size = batch_size
    extractor._service_account_file = None
    extractor._api_key = None
    extractor._model = MagicMock()
    return extractor


def nf_payload(quantidade: int, nfs: list[dict] | None = None) -> dict:
    return {
        "possui_nota_fiscal": quantidade > 0,
        "quantidade_notas_fiscais": quantidade,
        "notas_fiscais": nfs or [],
    }


class TestCachedResponsePath:
    def test_extract_from_pdf_uses_cache_and_skips_api_call(self, tmp_path: Path):
        extractor = make_extractor()
        cache_path = tmp_path / "doc_api_response.json"
        cache_path.write_text(
            json.dumps({"raw_text": json.dumps(nf_payload(1, [{"numero_nf": "1", "pagina": 1}]))}),
            encoding="utf-8",
        )

        result = extractor.extract_from_pdf(
            pdf_path=Path("doc.pdf"),
            pages=[1],
            save_api_response=True,
            api_response_output_dir=tmp_path,
        )

        assert result["cached"] is True
        assert result["quantidade_notas_fiscais"] == 1
        extractor.model.generate_content.assert_not_called()

    def test_extract_from_pdf_bytes_uses_cache_directly(self, tmp_path: Path):
        extractor = make_extractor()
        cache_path = tmp_path / "resp.json"
        cache_path.write_text(json.dumps({"raw_text": json.dumps(nf_payload(2))}), encoding="utf-8")

        result = extractor._extract_from_pdf_bytes(b"fake-pdf-bytes", num_pages=3, api_response_path=cache_path)

        assert result["cached"] is True
        assert result["quantidade_notas_fiscais"] == 2
        assert result["processed_successfully"] is True
        extractor.model.generate_content.assert_not_called()


class TestRetryOnZeroNfs:
    def test_retries_once_when_first_attempt_finds_zero_nfs(self, tmp_path: Path):
        extractor = make_extractor()
        extractor.model.generate_content.side_effect = [
            FakeGeminiResponse(nf_payload(0)),
            FakeGeminiResponse(nf_payload(1, [{"numero_nf": "42", "pagina": 1}])),
        ]

        result = extractor._extract_from_pdf_bytes(b"fake-pdf-bytes", num_pages=1)

        assert extractor.model.generate_content.call_count == 2
        assert result["quantidade_notas_fiscais"] == 1
        assert result["notas_fiscais"][0]["numero_nf"] == "42"

    def test_does_not_retry_when_first_attempt_finds_nfs(self, tmp_path: Path):
        extractor = make_extractor()
        extractor.model.generate_content.return_value = FakeGeminiResponse(
            nf_payload(1, [{"numero_nf": "1", "pagina": 1}])
        )

        result = extractor._extract_from_pdf_bytes(b"fake-pdf-bytes", num_pages=1)

        assert extractor.model.generate_content.call_count == 1
        assert result["quantidade_notas_fiscais"] == 1

    def test_gives_up_after_max_attempts_still_zero(self, tmp_path: Path):
        extractor = make_extractor()
        extractor.model.generate_content.return_value = FakeGeminiResponse(nf_payload(0))

        result = extractor._extract_from_pdf_bytes(b"fake-pdf-bytes", num_pages=1)

        assert extractor.model.generate_content.call_count == 2
        assert result["quantidade_notas_fiscais"] == 0

    def test_api_error_on_last_attempt_returns_failure_dict_not_raise(self, tmp_path: Path):
        extractor = make_extractor()
        extractor.model.generate_content.side_effect = RuntimeError("quota exceeded")

        result = extractor._extract_from_pdf_bytes(b"fake-pdf-bytes", num_pages=1)

        assert extractor.model.generate_content.call_count == 2
        assert result["processed_successfully"] is False
        assert "quota exceeded" in result["error"]
        assert result["notas_fiscais"] == []


class TestBatchingAndPageRemapping:
    def test_multi_batch_pagina_remapped_to_original_pdf_pages(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
        extractor = make_extractor(batch_size=2)

        # Avoid needing a real multi-page PDF on disk: stub out the pure PDF-slicing
        # helper (pdf.py) since this test is only about the page-remapping/coalescing
        # logic in api.py, not real PDF byte handling.
        monkeypatch.setattr(extractor, "_create_filtered_pdf", lambda pdf_path, pages: b"fake-bytes")

        batch_results = [
            nf_payload(2, [{"numero_nf": "A", "pagina": 1}, {"numero_nf": "B", "pagina": 2}]),
            nf_payload(1, [{"numero_nf": "C", "pagina": 1}]),
        ]
        monkeypatch.setattr(extractor, "_extract_from_pdf_bytes", MagicMock(side_effect=batch_results))

        result = extractor.extract_from_pdf(pdf_path=Path("multi.pdf"), pages=[2, 7, 9, 15])

        assert result["batching_used"] is True
        assert result["num_batches"] == 2
        by_numero = {nf["numero_nf"]: nf for nf in result["notas_fiscais"]}
        assert by_numero["A"]["pagina"] == 2  # batch 1, filtered index 1 -> original page 2
        assert by_numero["B"]["pagina"] == 7  # batch 1, filtered index 2 -> original page 7
        assert by_numero["C"]["pagina"] == 9  # batch 2, filtered index 1 -> original page 9
        assert result["quantidade_notas_fiscais"] == 3

    def test_single_call_pagina_remapped_when_pages_filtered(self, monkeypatch: pytest.MonkeyPatch):
        extractor = make_extractor(batch_size=5)
        monkeypatch.setattr(extractor, "_create_filtered_pdf", lambda pdf_path, pages: b"fake-bytes")
        extractor.model.generate_content.return_value = FakeGeminiResponse(
            nf_payload(1, [{"numero_nf": "X", "pagina": 1}])
        )

        result = extractor.extract_from_pdf(pdf_path=Path("single.pdf"), pages=[5])

        assert not result.get("batching_used")
        assert result["notas_fiscais"][0]["pagina"] == 5


class TestSuspiciousDecimalFallback:
    def test_fallback_model_triggered_and_result_returned(self, monkeypatch: pytest.MonkeyPatch):
        extractor = make_extractor(model_name="gemini-3.1-flash-lite", batch_size=5, prompt="PROMPT")
        # Patch at the class level (not just this instance): the suspicious-decimal
        # path constructs a brand-new `type(self)(...)` fallback extractor internally,
        # so the stub must apply to that new instance too.
        monkeypatch.setattr(NFExtractor, "_create_filtered_pdf", lambda self, pdf_path, pages: b"fake-bytes")
        # Suspicious: more than 2 decimal places.
        extractor.model.generate_content.return_value = FakeGeminiResponse(
            nf_payload(1, [{"numero_nf": "S", "pagina": 1, "valor_total": 12.12345}])
        )

        fallback_calls = []

        def fake_init(
            extractor, model_name=None, service_account_file=None, api_key=None, extraction_prompt=None, batch_size=5
        ):
            fallback_calls.append(model_name)
            extractor.model_name = model_name
            extractor.extraction_prompt = extraction_prompt
            extractor.batch_size = batch_size
            extractor._service_account_file = service_account_file
            extractor._api_key = api_key
            extractor._model = MagicMock()
            extractor._model.generate_content.return_value = FakeGeminiResponse(
                nf_payload(1, [{"numero_nf": "S", "pagina": 1, "valor_total": 12.12}])
            )

        monkeypatch.setattr(auth, "initialize", fake_init)

        result = extractor.extract_from_pdf(pdf_path=Path("suspicious.pdf"), pages=[1])

        assert fallback_calls == ["gemini-2.5-flash-lite"]
        assert result["notas_fiscais"][0]["valor_total"] == 12.12

    def test_no_fallback_when_already_on_fallback_model(self, monkeypatch: pytest.MonkeyPatch):
        extractor = make_extractor(model_name="gemini-2.5-flash-lite", batch_size=5, prompt="PROMPT")
        monkeypatch.setattr(extractor, "_create_filtered_pdf", lambda pdf_path, pages: b"fake-bytes")
        extractor.model.generate_content.return_value = FakeGeminiResponse(
            nf_payload(1, [{"numero_nf": "S", "pagina": 1, "valor_total": 12.12345}])
        )

        init_spy = MagicMock()
        monkeypatch.setattr(auth, "initialize", init_spy)

        result = extractor.extract_from_pdf(pdf_path=Path("suspicious.pdf"), pages=[1])

        init_spy.assert_not_called()
        assert result["notas_fiscais"][0]["valor_total"] == 12.12345


class TestCoalesceAndDecimalHelpers:
    def test_split_pages_into_batches(self):
        assert coalesce.split_pages_into_batches([1, 2, 3], batch_size=5) == [[1, 2, 3]]
        assert coalesce.split_pages_into_batches(list(range(1, 8)), batch_size=3) == [
            [1, 2, 3],
            [4, 5, 6],
            [7],
        ]

    def test_has_suspicious_decimals(self):
        assert coalesce.has_suspicious_decimals([{"valor_total": 12.12}]) is False
        assert coalesce.has_suspicious_decimals([{"valor_total": 12.12345}]) is True
        assert coalesce.has_suspicious_decimals([{"valor_total": 0.0}]) is False

    def test_coalesce_merges_duplicate_numero_and_prefers_largest_value(self):
        all_nfs = [
            {"numero_nf": "1", "valor_total": 100.0, "pagina": 3},
            {"numero_nf": "1", "valor_total": 150.0, "pagina": 1},
            {"numero_nf": "2", "valor_total": 50.0, "pagina": 2},
        ]

        coalesced = coalesce.coalesce_nfs_by_numero(all_nfs)

        by_numero = {nf["numero_nf"]: nf for nf in coalesced}
        assert len(coalesced) == 2
        assert by_numero["1"]["valor_total"] == 150.0  # larger value wins
        assert by_numero["1"]["pagina"] == 1  # earliest page wins
        assert "MERGE" in by_numero["1"]["observacao"]
