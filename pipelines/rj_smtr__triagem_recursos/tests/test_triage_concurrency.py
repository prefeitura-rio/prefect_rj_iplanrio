from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from threading import Lock
from time import sleep
from typing import final

import pymupdf as fitz
import pytest

from pipelines.rj_smtr__triagem_recursos.tests.conftest import (
    DummyCalibrator,
    DummyConformal,
    build_pdf_bytes,
)
from pipelines.rj_smtr__triagem_recursos.utils.feature_extractor import VetorSinaisEpistemicos
from pipelines.rj_smtr__triagem_recursos.utils.schemas import (
    AuditoriaSistema,
    Encaminhamento,
    EsquemaDecisao,
    HipoteseLegal,
    TipoCitacao,
)
from pipelines.rj_smtr__triagem_recursos.utils.triage import (
    classificar_documento_preparado,
    executar_lote_em_janelas,
    preparar_documento_processo,
)


@final
class FakeLLMClient:
    """Cliente LLM simulado que registra chamadas sem acesso externo."""

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def classificar(
        self,
        pdf_bytes: bytes,
        prompt_sistema: str,
        modelo: str | None = None,
        temperature: float = 0.0,
        reasoning_effort: str = "medium",
    ) -> tuple[EsquemaDecisao, dict[str, int]]:
        """Retorna decisão estruturada determinística e contagem fixa de tokens."""
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        page_count = len(doc)
        doc.close()
        self.calls.append(
            {
                "pages": page_count,
                "modelo": modelo,
                "temperature": temperature,
                "reasoning_effort": reasoning_effort,
                "prompt_len": len(prompt_sistema),
            }
        )
        decisao = EsquemaDecisao(
            hipotese_legal=HipoteseLegal.HIPOTESE_ESGOTADA,
            encaminhamento=Encaminhamento.PUBLICAR_INTEMPESTIVO,
            justificativa_analise_previa="Recurso de mérito puro sem hipótese excepcional.",
            pagina_evidencia=1,
            frase_justificativa="Requer cancelamento da multa.",
            tipo_citacao=TipoCitacao.ALEGACAO_HIPOTETICA,
            auditoria_sistema=AuditoriaSistema(
                prefixo_auto="RD",
                codigo_status=71,
                fase_descricao="PENALIDADE",
                possui_espelho_nada_consta=False,
            ),
        )
        return decisao, {"prompt_tokens": 1000, "completion_tokens": 120, "cached_tokens": 700}


def test_executar_lote_em_janelas_empty_returns_empty() -> None:
    """Retorna lista vazia imediatamente quando não há processos pendentes."""

    def submit_dl(proc: dict[str, object]) -> Future[dict[str, object]]:
        fut: Future[dict[str, object]] = Future()
        fut.set_result({"ok": True, "processo": proc})
        return fut

    def submit_cls(doc: dict[str, object]) -> Future[dict[str, object]]:
        fut: Future[dict[str, object]] = Future()
        fut.set_result({"processo_id": doc})
        return fut

    res = executar_lote_em_janelas(
        processos=[],
        submeter_download=submit_dl,
        submeter_classificacao=submit_cls,
        aguardar_concluidos=as_completed,
    )
    assert res == []


def test_executar_lote_em_janelas_warms_first_valid_and_limits_downloads() -> None:
    """Garante warmup do cache no 1o PDF válido, teto de downloads e sobreposição."""
    lock = Lock()
    timeline: list[str] = []
    active_downloads = 0
    max_observed_downloads = 0
    max_parallel_overlap = 0
    active_cls = 0

    processos: list[dict[str, object]] = [{"processo_id": f"p-{i}"} for i in range(8)]

    def fake_download(proc: dict[str, object]) -> dict[str, object]:
        nonlocal active_downloads, max_observed_downloads
        pid = str(proc["processo_id"])
        with lock:
            active_downloads += 1
            max_observed_downloads = max(max_observed_downloads, active_downloads)
            timeline.append(f"dl_start:{pid}")
        sleep(0.03)
        with lock:
            active_downloads -= 1
            timeline.append(f"dl_end:{pid}")
        if pid == "p-0":
            return {
                "ok": False,
                "processo": proc,
                "resultado_erro": {"processo_id": pid, "rota_execucao": "HITL_ERRO_ARQUIVO"},
            }
        return {"ok": True, "processo": proc}

    def fake_classify(doc: dict[str, object]) -> dict[str, object]:
        nonlocal active_cls, max_parallel_overlap
        proc_obj = doc["processo"]
        assert isinstance(proc_obj, dict)
        pid = str(proc_obj["processo_id"])
        with lock:
            active_cls += 1
            max_parallel_overlap = max(max_parallel_overlap, active_cls + active_downloads)
            timeline.append(f"cls_start:{pid}")
        sleep(0.05)
        with lock:
            active_cls -= 1
            timeline.append(f"cls_end:{pid}")
        return {"processo_id": pid, "rota_execucao": "N1_DIRETO"}

    with ThreadPoolExecutor(max_workers=8) as pool:
        resultados = executar_lote_em_janelas(
            processos=processos,
            submeter_download=lambda p: pool.submit(fake_download, p),
            submeter_classificacao=lambda d: pool.submit(fake_classify, d),
            aguardar_concluidos=as_completed,
            max_downloads_simultaneos=3,
            aquecer_primeiro=True,
        )

    assert len(resultados) == 8
    assert any(item["processo_id"] == "p-0" and item["rota_execucao"] == "HITL_ERRO_ARQUIVO" for item in resultados)
    assert any(item["processo_id"] == "p-1" and item["rota_execucao"] == "N1_DIRETO" for item in resultados)
    assert timeline.index("cls_end:p-1") < timeline.index("dl_start:p-2")
    assert max_observed_downloads <= 3
    assert max_parallel_overlap > 3


def test_two_stage_pipeline_truncates_and_classifies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Valida estágio 1 (Smart Window 5 pág.) + estágio 2 (classificação/custo)."""
    pdf_25 = build_pdf_bytes(num_pages=25)
    fake_llm = FakeLLMClient()

    @final
    class FakeSei:
        def obter_pdf_recurso_processo(self, id_proc: str) -> tuple[str, bytes]:
            return f"doc-{id_proc}", pdf_25

    def fake_get_llm(modelo: str | None = None) -> FakeLLMClient:
        _ = modelo
        return fake_llm

    def fake_ocr(**_kwargs: object) -> tuple[str, float]:
        return "Requer cancelamento da multa.", 0.92

    def fake_signals(**_kwargs: object) -> VetorSinaisEpistemicos:
        return VetorSinaisEpistemicos(
            fuzz_ratio=1.0,
            token_containment=1.0,
            exact_match=1.0,
            adversarial_density=0.0,
            sim_centroid_pred=0.9,
            mahalanobis_dist=1.0,
            is_manuscrito=0.0,
            tesseract_mean_conf=0.92,
        )

    monkeypatch.setattr("pipelines.rj_smtr__triagem_recursos.utils.triage.obter_cliente_llm", fake_get_llm)
    monkeypatch.setattr("pipelines.rj_smtr__triagem_recursos.utils.triage.extrair_ocr_pagina_especifica", fake_ocr)
    monkeypatch.setattr("pipelines.rj_smtr__triagem_recursos.utils.triage.extrair_sinais_epistemicos", fake_signals)

    artefatos: dict[str, object] = {
        "calibrador": DummyCalibrator(prob=0.96),
        "conformal": DummyConformal(),
        "prompt_sistema": "Instruções de triagem SMTR",
        "calibracao_id": "cal-1",
        "versao_prompt": "v4_baseline",
    }
    proc: dict[str, object] = {
        "processo_id": "uuid-1",
        "id_procedimento_sei": "555",
        "numero_processo_sei": "000351.000001/2026-00",
    }

    doc_prep = preparar_documento_processo(
        processo=proc,
        artefatos=artefatos,
        fonte_pdf="sei",
        cliente_sei=FakeSei(),
        max_paginas=5,
        modelo_llm="vertex/gemini-3.7-flash",
    )
    assert doc_prep["ok"] is True

    resultado = classificar_documento_preparado(
        documento_preparado=doc_prep,
        artefatos=artefatos,
        modelo_llm="vertex/gemini-3.7-flash",
        reasoning_effort="medium",
        executar_n2=True,
    )

    assert fake_llm.calls[0]["pages"] == 5
    assert fake_llm.calls[0]["reasoning_effort"] == "medium"
    assert resultado["faixa_operacional"] == "VERDE"
    assert resultado["rota_execucao"] == "N1_DIRETO"
    assert resultado["tokens_prompt_total"] == 1000
    assert resultado["tokens_cached"] == 700
