"""Tests for extracao_pagina row building."""

from datetime import datetime

from pipelines.rj_iplanrio__nf_agent.utils.output import RunMetadata, build_extracao_pagina_rows, build_versao_pipeline
from pipelines.rj_iplanrio__nf_agent.utils.results import PdfResult

USAGE = {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3}
# Naive datetime on purpose: build_extracao_pagina_rows uses utc_now_naive() + isoformat() + "Z",
# not a tz-aware ISO string, so this mirrors production instead of an accidental omission.
META = RunMetadata({"versao_processamento": "auto-abc", "commit": "x"}, datetime(2026, 9, 24, 12, 0, 0))  # noqa: DTZ001


def result(**overrides) -> PdfResult:
    base = {
        "pdf_name": "doc",
        "total_pages": 3,
        "categories": {1: "Nenhuma das Opções", 2: "NFS-e"},
        "justifications": {1: "j1", 2: "j2"},
        "classification_usage": {1: USAGE, 2: USAGE},
        "extraction_usage": {2: USAGE},
        "classification_errors": {},
        "extraction_errors": {},
        "extracted_nfs": [{"pagina": 2, "tipo_documento": "NFS-e", "numero_nf": "10", "valor_total": 5.0}],
    }
    base.update(overrides)
    return PdfResult(**base)


def test_one_row_per_page_with_version_fields():
    rows = build_extracao_pagina_rows({"doc": result()}, META)
    assert [row["pagina"] for row in rows] == [1, 2, 3]
    assert all(row["versao_pipeline"]["versao_processamento"] == "auto-abc" for row in rows)
    assert "versao_processamento" not in rows[0]
    assert all(row["timestamp_geracao"] == "2026-09-24T12:00:00Z" for row in rows)
    assert rows[0]["pipeline_status"] == "ok"
    assert rows[0]["numero_documento"] is None
    assert rows[1]["numero_documento"] == "10"
    assert rows[1]["uso"]["extracao"]["total_tokens"] == USAGE["total_tokens"]
    assert rows[2]["pipeline_status"] == "erro_processamento"
    assert rows[2]["pipeline_erro"] == "Página não processada"


def test_errors_become_erro_processamento():
    rows = build_extracao_pagina_rows(
        {"doc": result(total_pages=2, extracted_nfs=[], extraction_errors={2: "timeout"}, classification_errors={})},
        META,
    )
    assert rows[1]["pipeline_status"] == "erro_processamento"
    assert rows[1]["pipeline_erro"] == "timeout"
    assert rows[1]["tipo_documento_classificacao"] == "NFS-e"


def test_classification_error_message_is_kept():
    rows = build_extracao_pagina_rows(
        {"doc": result(total_pages=3, classification_errors={3: "Resposta sem texto"})}, META
    )
    assert rows[2]["pipeline_erro"] == "Resposta sem texto"


def test_build_versao_pipeline(monkeypatch):
    monkeypatch.setenv("GIT_COMMIT_SHA", "abc1234")
    info = build_versao_pipeline("auto-abc", "v8", "v9", "run-1", "sess-1")
    assert info == {
        "versao_processamento": "auto-abc",
        "commit": "abc1234",
        "modelo": "gemini-3.1-flash-lite",
        "versao_prompt_classificacao": "v8",
        "versao_prompt_extracao": "v9",
        "run_id": "run-1",
        "session_id": "sess-1",
    }
