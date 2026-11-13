from collections.abc import Iterator
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from datetime import date
from threading import Lock
from time import sleep
from typing import Any, final

import pymupdf as fitz
import pytest
from httpx import Client, MockTransport, Request, Response

from pipelines.rj_smtr__triagem_recursos import constants
from pipelines.rj_smtr__triagem_recursos.constants import StatusProcessamento, obter_env, resolver_banco_url
from pipelines.rj_smtr__triagem_recursos.helpers import db_repository, triage
from pipelines.rj_smtr__triagem_recursos.helpers.feature_extractor import VetorSinaisEpistemicos
from pipelines.rj_smtr__triagem_recursos.helpers.ocr_engine import extrair_ocr_pagina_especifica
from pipelines.rj_smtr__triagem_recursos.helpers.sei_client import ModWsseiClient, SeiConfig
from pipelines.rj_smtr__triagem_recursos.helpers.triage import (
    classificar_documento_preparado,
    configurar_semaforo_sei,
    executar_lote_com_checkpoint,
    preparar_documento_processo,
    triar_processo,
)
from pipelines.rj_smtr__triagem_recursos.helpers.windowing import extrair_smart_window_pdf
from pipelines.rj_smtr__triagem_recursos.tests.conftest import (
    DummyCalibrator,
    DummyConformal,
    build_pdf_bytes,
    json_response,
)
from pipelines.rj_smtr__triagem_recursos.tests.test_triage_concurrency import FakeLLMClient


def _make_sei_config() -> SeiConfig:
    """Cria configuração isolada do cliente SEI para testes offline."""
    return SeiConfig(
        base_url="https://sei.local/api/v2",
        usuario="bot_test",
        senha="secret",
        id_unidade="1100001",
        orgao=12,
        timeout=15,
    )


def _fake_signals(**_kwargs: object) -> VetorSinaisEpistemicos:
    """Retorna vetor determinístico de sinais sem carregar embeddings em disco."""
    return VetorSinaisEpistemicos(
        fuzz_ratio=1.0,
        token_containment=1.0,
        exact_match=1.0,
        adversarial_density=0.0,
        sim_centroid_pred=0.9,
        mahalanobis_dist=1.0,
        is_manuscrito=0.0,
        tesseract_mean_conf=0.95,
    )


def test_obter_env_precedence_and_quote_stripping(monkeypatch: pytest.MonkeyPatch) -> None:
    """Valida prioridade SMTR_TRIAGEM__ > SMTR_TRIAGEM_ > sem prefixo e remoção de aspas."""
    monkeypatch.setenv("CHAVE_TESTE", "sem_prefixo")
    monkeypatch.setenv("SMTR_TRIAGEM_CHAVE_TESTE", "'com_prefixo_simples'")
    monkeypatch.setenv("SMTR_TRIAGEM__CHAVE_TESTE", '  "valor_prefixed"  ')

    assert obter_env("CHAVE_TESTE") == "valor_prefixed"

    monkeypatch.setenv("SMTR_TRIAGEM__CHAVE_TESTE", '""')
    assert obter_env("CHAVE_TESTE") == "com_prefixo_simples"

    monkeypatch.delenv("SMTR_TRIAGEM__CHAVE_TESTE", raising=False)
    monkeypatch.delenv("SMTR_TRIAGEM_CHAVE_TESTE", raising=False)
    assert obter_env("CHAVE_TESTE") == "sem_prefixo"
    assert obter_env("CHAVE_INEXISTENTE_XYZ", default="padrao") == "padrao"


def test_resolver_banco_url_prefers_prefixed_and_fails_fast(monkeypatch: pytest.MonkeyPatch) -> None:
    """Garante que SMTR_TRIAGEM__DATABASE_URL evita colisão com DATABASE_URL global."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://global_prefect:pw@other/prefect")
    monkeypatch.setenv("SMTR_TRIAGEM__DATABASE_URL", "postgresql://smtr:pw@cloudsql/smtr_staging")
    assert resolver_banco_url(is_local=False) == "postgresql://smtr:pw@cloudsql/smtr_staging"

    monkeypatch.delenv("SMTR_TRIAGEM__DATABASE_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("SMTR_TRIAGEM__DB_URL_CLOUD", raising=False)
    monkeypatch.delenv("DB_URL_CLOUD", raising=False)
    monkeypatch.setattr(constants, "DB_URL_LOCAL", "postgresql://local/test")

    assert resolver_banco_url(is_local=True) == "postgresql://local/test"
    with pytest.raises(RuntimeError, match="URL do Cloud SQL ausente"):
        resolver_banco_url(is_local=False)


def test_iterar_paginas_caixa_uses_zero_based_pages_and_deduplicates() -> None:
    """Verifica paginação 0-based (start=0,1,...), formatos dict/list e deduplicação."""
    requested_starts: list[str] = []

    def handler(request: Request) -> Response:
        requested_starts.append(request.url.params.get("start", ""))
        start_val = int(request.url.params.get("start", "0"))
        if start_val == 0:
            return json_response(
                {
                    "sucesso": True,
                    "data": {
                        "total": 3,
                        "processos": [
                            {"idProcedimento": "1", "protocoloFormatado": "SEI-001", "dataGeracao": "01/02/2026"},
                            {"idProcedimento": "2", "protocoloFormatado": "SEI-002", "dataGeracao": "02/02/2026"},
                        ],
                    },
                }
            )
        if start_val == 1:
            # Simula sobreposição do item id=2 na fronteira entre páginas e formato list direto
            return json_response(
                {
                    "sucesso": True,
                    "total_itens": 3,
                    "data": [
                        {"idProcedimento": "2", "protocoloFormatado": "SEI-002"},
                        {"idProcedimento": "3", "protocoloFormatado": "SEI-003"},
                    ],
                }
            )
        return json_response({"sucesso": True, "data": []})

    http_client = Client(transport=MockTransport(handler))
    sei = ModWsseiClient(config=_make_sei_config(), http_client=http_client)
    sei.token = "tok-valido"

    paginas = list(sei.iterar_paginas_caixa(limit=2, max_paginas=10))
    http_client.close()

    assert requested_starts == ["0", "1"]
    assert [[p["numero_processo_sei"] for p in pg] for pg in paginas] == [
        ["SEI-001", "SEI-002"],
        ["SEI-003"],
    ]


def test_obter_pdf_recurso_com_metadados_skips_cancelled_and_returns_protocol() -> None:
    """Garante retorno do protocolo formatado e ignorância de documentos cancelados."""
    pdf_p1 = build_pdf_bytes(num_pages=1, text_prefix="Peticao valida")
    pdf_p2 = build_pdf_bytes(num_pages=1, text_prefix="Anexo valido")

    def handler(request: Request) -> Response:
        if request.url.path.endswith("/documento/listar/999"):
            return json_response(
                {
                    "sucesso": True,
                    "data": {
                        "documentos": [
                            {
                                "idProtocolo": "10",
                                "atributos": {
                                    "protocoloFormatado": "00010",
                                    "tipoDocumento": "E",
                                    "nomeComposto": "Recurso Cancelado.pdf",
                                    "mimeType": "application/pdf",
                                    "status": {"documentoCancelado": "S"},
                                },
                            },
                            {
                                "idProtocolo": "11",
                                "atributos": {
                                    "protocoloFormatado": "0012345",
                                    "tipoDocumento": "E",
                                    "nomeComposto": "Recurso.pdf",
                                    "mimeType": "application/pdf",
                                    "status": {"documentoCancelado": "N"},
                                },
                            },
                            {
                                "idProtocolo": "12",
                                "atributos": {
                                    "protocoloFormatado": "0012346",
                                    "tipoDocumento": "X",
                                    "nomeComposto": "Anexo.pdf",
                                    "mimeType": "application/pdf",
                                    "status": {"documentoCancelado": "N"},
                                },
                            },
                        ]
                    },
                }
            )
        if request.url.path.endswith("/documento/baixar/anexo/11"):
            return Response(status_code=200, content=pdf_p1)
        if request.url.path.endswith("/documento/baixar/anexo/12"):
            return Response(status_code=200, content=pdf_p2)
        return Response(status_code=404)

    http_client = Client(transport=MockTransport(handler))
    sei = ModWsseiClient(config=_make_sei_config(), http_client=http_client)
    sei.token = "tok-valido"

    ids_csv, protocolo, pdf_unido = sei.obter_pdf_recurso_com_metadados(999)
    http_client.close()

    assert ids_csv == "11,12"
    assert protocolo == "0012345"
    doc = fitz.open(stream=pdf_unido, filetype="pdf")
    try:
        assert len(doc) == 2
    finally:
        doc.close()


@final
class _FakeCursor:
    """Cursor psycopg2 em memória para testes do repositório sem PostgreSQL real."""

    def __init__(self, state: dict[str, Any]) -> None:
        self.state = state
        self._rows: list[Any] = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, query: str, params: Any = None) -> None:
        self.state["executed"].append((query, params))
        if "FROM information_schema.columns" in query:
            self._rows = [(1,)] if self.state.get("has_protocolo_col") else []
            return
        if "ANY(%s)" in query:
            candidatos = list(params[0]) if params else []
            triados = self.state.get("triados", set())
            self._rows = [(c,) for c in candidatos if c in triados]
            return
        if "INSERT INTO" in query and "RETURNING" in query:
            num_sei = params[0]
            self.state["upserts"].append(params)
            self._rows = [(f"uuid-{num_sei}",)]
            return
        if "INSERT INTO" in query:
            self.state["enqueues"].append(params)
            self._rows = []
            return
        if "UPDATE" in query:
            self.state["updates"].append((query, params))
            self._rows = []

    def fetchall(self) -> list[Any]:
        return list(self._rows)

    def fetchone(self) -> Any:
        return self._rows[0] if self._rows else None


@final
class _FakeConnection:
    """Conexão psycopg2 em memória para verificar transações e commits."""

    def __init__(self, state: dict[str, Any]) -> None:
        self.state = state

    def cursor(self, **_kwargs: Any) -> _FakeCursor:
        return _FakeCursor(self.state)

    def commit(self) -> None:
        self.state["commits"] += 1

    def rollback(self) -> None:
        self.state["rollbacks"] += 1

    def close(self) -> None:
        self.state["closes"] += 1


def test_buscar_processos_sei_paginates_until_limit_and_persists_data_autuacao(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Paginação no SEI deve pular páginas com processos já triados até completar limite."""
    state: dict[str, Any] = {
        "triados": {"SEI-001", "SEI-002"},
        "has_protocolo_col": False,
        "executed": [],
        "upserts": [],
        "enqueues": [],
        "updates": [],
        "commits": 0,
        "rollbacks": 0,
        "closes": 0,
    }
    monkeypatch.setattr(db_repository.psycopg2, "connect", lambda _url: _FakeConnection(state))

    class _FakePaginatedSEI:
        def iterar_paginas_caixa(self, limit: int = 100) -> Iterator[list[dict[str, Any]]]:
            assert limit >= 1
            yield [
                {"id_procedimento_sei": "1", "numero_processo_sei": "SEI-001", "data_autuacao": "01/01/2026"},
                {"id_procedimento_sei": "2", "numero_processo_sei": "SEI-002", "data_autuacao": "02/01/2026"},
            ]
            yield [
                {"id_procedimento_sei": "3", "numero_processo_sei": "SEI-003", "data_autuacao": "15/03/2026"},
                {"id_procedimento_sei": "4", "numero_processo_sei": "SEI-004", "data_autuacao": "2026-03-20"},
            ]
            yield [
                {"id_procedimento_sei": "5", "numero_processo_sei": "SEI-005", "data_autuacao": "invalid"},
            ]

    selecionados = db_repository.buscar_processos_para_triagem_ou_fallback(
        db_url="postgresql://mock/db",
        is_local=False,
        fonte_pdf="sei",
        cliente_sei=_FakePaginatedSEI(),
        limite=2,
    )

    assert [p["numero_processo_sei"] for p in selecionados] == ["SEI-003", "SEI-004"]
    assert state["upserts"][0][5] == date(2026, 3, 15)
    assert state["upserts"][1][5] == date(2026, 3, 20)
    assert state["commits"] == 1
    assert state["rollbacks"] == 0
    assert state["closes"] == 1


def _build_mock_result(processo_id: str, is_error: bool = False) -> dict[str, Any]:
    """Constrói resultado sintético de triagem para testar persistência em lote."""
    status_val = (
        StatusProcessamento.ERRO_PROCESSAMENTO.value if is_error else StatusProcessamento.CONCLUIDO_IA.value
    )
    return {
        "processo_id": processo_id,
        "id_documento_sei": "12345",
        "protocolo_documento_sei": "0012345",
        "calibracao_id": 1,
        "modelo_llm": "vertex/gemini-3.7-flash",
        "versao_prompt": "v4_baseline",
        "n1_classe_predita": "OUTROS",
        "n1_encaminhamento": "REVISAO_HUMANA",
        "n1_cot_justificativa": "PDF corrompido no SEI" if is_error else "Merito intempestivo.",
        "n1_lauda_evidencia": 1,
        "n1_frase_citada": "",
        "n1_tipo_citacao": "DOCUMENTO_ILEGIVEL" if is_error else "ALEGACAO_HIPOTETICA",
        "sinais_epistemicos": {},
        "auditoria_sistema": {"erro_detalhado": "Timeout 504 no mod-wssei"} if is_error else {},
        "probabilidade_calibrada": 0.0 if is_error else 0.95,
        "conjunto_conformal": ["OUTROS"],
        "tamanho_conjunto_conformal": 1,
        "estagio2_executado": False,
        "n2_classe_predita": None,
        "n2_cot_justificativa": None,
        "n2_delta_cot_dispersao": None,
        "n2_log_odds_atualizado": None,
        "probabilidade_final": 0.0 if is_error else 0.95,
        "rota_execucao": "HITL_ERRO_ARQUIVO" if is_error else "N1_DIRETO",
        "faixa_operacional": "VERMELHA" if is_error else "VERDE",
        "status_fila": status_val,
        "tokens_prompt_total": 0 if is_error else 500,
        "tokens_completion": 0 if is_error else 50,
        "tokens_cached": 0,
        "custo_real_usd": 0.0,
        "custo_sem_cache_usd": 0.0,
        "economia_cache_usd": 0.0,
        "tempo_execucao_s": 0.5,
    }


def test_persistir_lote_separa_erros_sem_predicao_falsa_e_atualiza_protocolo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Erros não entram em predicoes_pipeline e protocolo só atualiza se coluna existir."""
    captured_batches: list[tuple[str, list[Any]]] = []

    def fake_execute_values(_cursor: Any, query: str, argslist: Any) -> None:
        captured_batches.append((query, list(argslist)))

    state: dict[str, Any] = {
        "has_protocolo_col": True,
        "executed": [],
        "upserts": [],
        "enqueues": [],
        "updates": [],
        "commits": 0,
        "rollbacks": 0,
        "closes": 0,
    }
    monkeypatch.setattr(db_repository.psycopg2, "connect", lambda _url: _FakeConnection(state))
    monkeypatch.setattr(db_repository, "execute_values", fake_execute_values)

    db_repository.persistir_lote_resultados_ia(
        db_url="postgresql://mock/db",
        resultados=[
            _build_mock_result("proc-ok", is_error=False),
            _build_mock_result("proc-err", is_error=True),
        ],
    )

    pred_batches = [vals for q, vals in captured_batches if "predicoes_pipeline" in q]
    erro_batches = [vals for q, vals in captured_batches if "ultima_mensagem_erro" in q]
    proto_batches = [vals for q, vals in captured_batches if "protocolo_documento_sei" in q]

    assert len(pred_batches) == 1
    assert [row[0] for row in pred_batches[0]] == ["proc-ok"]
    assert len(erro_batches) == 1
    assert erro_batches[0] == [("proc-err", "ERRO_PROCESSAMENTO", "Timeout 504 no mod-wssei")]
    assert len(proto_batches) == 1
    assert proto_batches[0] == [("proc-ok", "0012345"), ("proc-err", "0012345")]
    assert state["commits"] == 1

    # Sem a coluna protocolo_documento_sei, não deve executar o UPDATE condicional
    captured_batches.clear()
    state["has_protocolo_col"] = False
    db_repository.persistir_lote_resultados_ia(
        db_url="postgresql://mock/db",
        resultados=[_build_mock_result("proc-ok", is_error=False)],
    )
    assert not any("protocolo_documento_sei" in q for q, _ in captured_batches)


def test_preparar_e_classificar_liberam_bytes_e_propagam_protocolo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Preparo libera pdf_bytes após windowing e classificação consome pdf_window_bytes."""
    pdf_full = build_pdf_bytes(num_pages=6, text_prefix="Recurso")

    class _FakeSeiDocClient:
        def obter_pdf_recurso_com_metadados(self, _pid: str) -> tuple[str, str, bytes]:
            return "888", "0099887", pdf_full

    processo = {
        "processo_id": "p-1",
        "id_procedimento_sei": "5001",
        "numero_processo_sei": "SEI-5001",
    }
    artefatos: dict[str, Any] = {
        "calibrador": DummyCalibrator(prob=0.96),
        "conformal": DummyConformal(alpha=0.05),
        "prompt_sistema": "Prompt sistema.",
        "calibracao_id": 1,
        "versao_prompt": "v4_baseline",
    }

    doc_prep = preparar_documento_processo(
        processo=processo,
        artefatos=artefatos,
        fonte_pdf="sei",
        cliente_sei=_FakeSeiDocClient(),
        max_paginas=2,
    )

    assert doc_prep["ok"] is True
    assert "pdf_bytes" not in doc_prep
    assert "pdf_window_bytes" in doc_prep
    assert doc_prep["protocolo_documento_sei"] == "0099887"

    fake_llm = FakeLLMClient()
    monkeypatch.setattr(triage, "obter_cliente_llm", lambda modelo=None: fake_llm)
    monkeypatch.setattr(triage, "extrair_ocr_pagina_especifica", lambda **_kw: ("Requer cancelamento da multa.", 0.97))
    monkeypatch.setattr(triage, "extrair_sinais_epistemicos", _fake_signals)

    resultado = classificar_documento_preparado(doc_prep, artefatos=artefatos)
    assert "pdf_window_bytes" not in doc_prep
    assert resultado["protocolo_documento_sei"] == "0099887"
    assert resultado["id_documento_sei"] == "888"


def test_executar_lote_com_checkpoint_aquece_e_persiste_incrementalmente() -> None:
    """Confirma warmup no 1o processo válido, waves e flush incremental a cada lote."""
    processos = [{"processo_id": f"p-{i}"} for i in range(6)]
    processos[0]["falha_warmup"] = True
    eventos: list[str] = []
    lotes_salvos: list[list[str]] = []

    def submeter(proc: dict[str, Any]) -> Future[dict[str, Any]]:
        fut: Future[dict[str, Any]] = Future()
        pid = proc["processo_id"]
        eventos.append(f"start:{pid}")
        fut.set_result(
            {
                "processo_id": pid,
                "status_fila": (
                    StatusProcessamento.ERRO_PROCESSAMENTO.value
                    if proc.get("falha_warmup")
                    else StatusProcessamento.CONCLUIDO_IA.value
                ),
            }
        )
        return fut

    resultados = executar_lote_com_checkpoint(
        processos=processos,
        submeter_processo=submeter,
        aguardar_concluidos=as_completed,
        ao_concluir_lote=lambda lote: lotes_salvos.append([str(r["processo_id"]) for r in lote]),
        tamanho_lote_persistencia=2,
        tamanho_onda=3,
        aquecer_primeiro=True,
    )

    assert len(resultados) == 6
    # Como p-0 deu ERRO_PROCESSAMENTO, p-1 também executou no warmup antes de liberar a onda [p-2, p-3, p-4]
    assert eventos[:2] == ["start:p-0", "start:p-1"]
    assert [len(lote) for lote in lotes_salvos] == [2, 2, 2]
    assert lotes_salvos[0] == ["p-0", "p-1"]
    assert sorted(pid for lote in lotes_salvos for pid in lote) == [f"p-{i}" for i in range(6)]


def test_semaforo_sei_limita_downloads_simultaneos_em_triar_processo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Garante que configurar_semaforo_sei limita downloads concorrentes sem bloquear inferência."""
    lock = Lock()
    estado = {"ativos": 0, "pico": 0}
    pdf_mock = build_pdf_bytes(num_pages=1, text_prefix="Recurso")

    class _FakeSeiConcurrent:
        def obter_pdf_recurso_com_metadados(self, pid: str) -> tuple[str, str, bytes]:
            with lock:
                estado["ativos"] += 1
                estado["pico"] = max(estado["pico"], estado["ativos"])
            sleep(0.03)
            with lock:
                estado["ativos"] -= 1
            return f"doc-{pid}", f"000{pid}", pdf_mock

    artefatos: dict[str, Any] = {
        "calibrador": DummyCalibrator(prob=0.96),
        "conformal": DummyConformal(alpha=0.05),
        "prompt_sistema": "Prompt",
        "calibracao_id": 1,
        "versao_prompt": "v4_baseline",
    }
    monkeypatch.setattr(triage, "obter_cliente_llm", lambda modelo=None: FakeLLMClient())
    monkeypatch.setattr(triage, "extrair_ocr_pagina_especifica", lambda **_kw: ("Texto", 0.95))
    monkeypatch.setattr(triage, "extrair_sinais_epistemicos", _fake_signals)

    sei_client = _FakeSeiConcurrent()
    configurar_semaforo_sei(2)
    try:
        processos = [
            {"processo_id": f"p-{i}", "id_procedimento_sei": str(i), "numero_processo_sei": f"SEI-{i}"}
            for i in range(6)
        ]
        with ThreadPoolExecutor(max_workers=6) as pool:
            resultados = list(
                pool.map(
                    lambda proc: triar_processo(
                        processo=proc,
                        artefatos=artefatos,
                        fonte_pdf="sei",
                        cliente_sei=sei_client,
                    ),
                    processos,
                )
            )
    finally:
        configurar_semaforo_sei(None)

    assert len(resultados) == 6
    assert estado["pico"] <= 2


def test_fitz_lock_multithread_windowing_and_ocr() -> None:
    """Valida estabilidade thread-safe de PyMuPDF sob janela + OCR concorrentes."""
    pdf_bytes = build_pdf_bytes(num_pages=5, text_prefix="Auto infracao defensiva suficiente para vetor")

    def worker(_idx: int) -> tuple[int, str, float]:
        win = extrair_smart_window_pdf(pdf_bytes, max_paginas=2)
        doc = fitz.open(stream=win, filetype="pdf")
        try:
            qtd_pags = len(doc)
        finally:
            doc.close()
        texto, conf = extrair_ocr_pagina_especifica(win, num_pagina=1)
        return qtd_pags, texto, conf

    with ThreadPoolExecutor(max_workers=8) as pool:
        outputs = list(pool.map(worker, range(16)))

    assert all(pags == 2 for pags, _, _ in outputs)
    assert all("Auto infracao" in texto and conf >= 0.9 for _, texto, conf in outputs)
