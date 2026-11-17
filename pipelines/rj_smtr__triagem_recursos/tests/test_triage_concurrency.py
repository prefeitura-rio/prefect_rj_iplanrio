from typing import final

import pymupdf as fitz
import pytest

from pipelines.rj_smtr__triagem_recursos.helpers.feature_extractor import VetorSinaisEpistemicos
from pipelines.rj_smtr__triagem_recursos.helpers.schemas import (
    AuditoriaSistema,
    Encaminhamento,
    EsquemaDecisao,
    HipoteseLegal,
    TipoCitacao,
)
from pipelines.rj_smtr__triagem_recursos.helpers.triage import (
    classificar_documento_preparado,
    preparar_documento_processo,
)
from pipelines.rj_smtr__triagem_recursos.tests.conftest import (
    DummyCalibrator,
    DummyConformal,
    build_pdf_bytes,
)


@final
class FakeLLMClient:
    """Cliente LLM simulado que registra chamadas sem acesso externo."""

    def __init__(self) -> None:
        """Inicializa lista de chamadas registradas."""
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

    monkeypatch.setattr("pipelines.rj_smtr__triagem_recursos.helpers.triage.obter_cliente_llm", fake_get_llm)
    monkeypatch.setattr("pipelines.rj_smtr__triagem_recursos.helpers.triage.extrair_ocr_pagina_especifica", fake_ocr)
    monkeypatch.setattr("pipelines.rj_smtr__triagem_recursos.helpers.triage.extrair_sinais_epistemicos", fake_signals)

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
