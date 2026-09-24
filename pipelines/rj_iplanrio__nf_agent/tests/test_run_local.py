"""Tests for the local CLI entrypoint."""

import importlib.util
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from pipelines.rj_iplanrio__nf_agent.utils.direct import DirectResult
from pipelines.rj_iplanrio__nf_agent.utils.llm_requests import PageId
from pipelines.rj_iplanrio__nf_agent.utils.prompts import PromptSet
from pipelines.rj_iplanrio__nf_agent.utils.responses import ClassificationResult

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "run_local.py"
spec = importlib.util.spec_from_file_location("run_local", SCRIPT)
run_local = importlib.util.module_from_spec(spec)
spec.loader.exec_module(run_local)


def test_run_writes_one_row_per_page(tmp_path, pdf_bytes):
    pdf = tmp_path / "doc.pdf"
    page_count = 2
    pdf.write_bytes(pdf_bytes(page_count))
    output = tmp_path / "out.ndjson"
    usage = {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}
    result = DirectResult(
        page_count,
        [ClassificationResult(PageId("doc", n), "Nenhuma das Opções", "j", usage, None) for n in (1, 2)],
        [],
    )
    with (
        patch.object(run_local, "load_prompts", return_value=PromptSet("v1", "c", "v1", "e")),
        patch.object(run_local, "process_pdf_direct", return_value=result),
    ):
        count = run_local.run([pdf], output, MagicMock())
    rows = [json.loads(line) for line in output.read_text().splitlines()]
    assert count == page_count
    assert [row["pagina"] for row in rows] == [1, 2]
    assert rows[0]["nome_arquivo"] == "doc"
    assert rows[0]["versao_pipeline"]["session_id"] is None


def test_run_skips_unreadable_pdf_and_keeps_going(tmp_path, pdf_bytes):
    bad_pdf = tmp_path / "bad.pdf"
    bad_pdf.write_bytes(b"isto nao e um pdf")
    good_pdf = tmp_path / "good.pdf"
    good_pdf.write_bytes(pdf_bytes(1))
    output = tmp_path / "out.ndjson"
    usage = {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}
    good_result = DirectResult(
        1,
        [ClassificationResult(PageId("good", 1), "Nenhuma das Opções", "j", usage, None)],
        [],
    )
    with (
        patch.object(run_local, "load_prompts", return_value=PromptSet("v1", "c", "v1", "e")),
        patch.object(
            run_local,
            "process_pdf_direct",
            side_effect=[ValueError("PDF ilegível"), good_result],
        ),
    ):
        count = run_local.run([bad_pdf, good_pdf], output, MagicMock())
    rows = [json.loads(line) for line in output.read_text().splitlines()]
    assert count == 1
    assert [row["nome_arquivo"] for row in rows] == ["good"]
