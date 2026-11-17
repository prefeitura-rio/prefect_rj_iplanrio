from concurrent.futures import ThreadPoolExecutor
from threading import Lock

from logging import INFO

import pymupdf as fitz
import pytest
from httpx import Client, MockTransport, Request, Response

from pipelines.rj_smtr__triagem_recursos.helpers import garantir_nivel_logs_pipeline
from pipelines.rj_smtr__triagem_recursos.helpers.sei_client import ModWsseiClient, SeiConfig
from pipelines.rj_smtr__triagem_recursos.tests.conftest import build_pdf_bytes, json_response
from prefect_rj_iplanrio.logging import get_logger


def _make_config(timeout: int = 15) -> SeiConfig:
    """Cria configuração isolada do cliente SEI para testes offline."""
    return SeiConfig(
        base_url="https://sei.local/api/v2",
        usuario="bot_test",
        senha="secret",
        id_unidade="1100001",
        orgao=12,
        timeout=timeout,
    )


def test_http_client_pool_is_reused_and_respects_ownership() -> None:
    """Garante reuso de um único httpx.Client e respeito ao ciclo de posse."""
    calls: list[str] = []

    def handler(request: Request) -> Response:
        calls.append(request.url.path)
        if request.url.path.endswith("/autenticar"):
            return json_response({"sucesso": True, "data": {"token": "jwt-1", "loginData": {"sigla": "BOT"}}})
        return json_response({"sucesso": True, "data": {"ok": True}})

    injected = Client(transport=MockTransport(handler))
    client_injected = ModWsseiClient(config=_make_config(), http_client=injected)
    client_injected.autenticar()
    client_injected.executar_requisicao("GET", "/processo/consultar/1")
    client_injected.close()

    assert not injected.is_closed
    assert calls == ["/api/v2/autenticar", "/api/v2/processo/consultar/1"]
    injected.close()

    owned = ModWsseiClient(config=_make_config())
    pool = owned.http_client
    assert owned.http_client is pool
    owned.close()
    assert pool.is_closed


def _list_docs_worker(sei_and_pid: tuple[ModWsseiClient, int]) -> list[dict[str, object]]:
    """Worker auxiliar tipado para consultar documentos em paralelo."""
    client, pid = sei_and_pid
    return client.listar_documentos_processo(pid)


def test_token_renewal_under_concurrency_authenticates_once() -> None:
    """Confirma que expiração 401 concorrente gera apenas um POST /autenticar."""
    auth_calls = 0
    auth_lock = Lock()
    state = {"active_token": "tok-expirado"}

    def handler(request: Request) -> Response:
        nonlocal auth_calls
        if request.url.path.endswith("/autenticar"):
            with auth_lock:
                auth_calls += 1
                state["active_token"] = f"tok-novo-{auth_calls}"
                tok = state["active_token"]
            return json_response({"sucesso": True, "data": {"token": tok}})

        if request.headers.get("token") == "tok-expirado":
            return Response(status_code=401, json={"sucesso": False})
        return json_response({"sucesso": True, "data": {"documentos": []}})

    http_client = Client(transport=MockTransport(handler))
    sei = ModWsseiClient(config=_make_config(), http_client=http_client)
    sei.token = "tok-expirado"

    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(_list_docs_worker, [(sei, pid) for pid in range(6)]))

    assert results == [[] for _ in range(6)]
    assert auth_calls == 1
    assert sei.token == "tok-novo-1"
    http_client.close()


def test_baixar_documento_recovers_from_401_and_504_streaming() -> None:
    """Valida recuperação de stream após 401 (reauth) e 504 antes de baixar PDF."""
    pdf_expected = build_pdf_bytes(num_pages=2)
    counters = {"auth": 0, "download": 0}

    def handler(request: Request) -> Response:
        if request.url.path.endswith("/autenticar"):
            counters["auth"] += 1
            return json_response({"sucesso": True, "data": {"token": "tok-valido"}})

        counters["download"] += 1
        if counters["download"] == 1:
            return Response(status_code=401)
        if counters["download"] == 2:
            return Response(status_code=504)
        return Response(status_code=200, content=pdf_expected)

    http_client = Client(transport=MockTransport(handler))
    sei = ModWsseiClient(config=_make_config(), http_client=http_client)
    sei.token = "tok-antigo"

    downloaded = sei.baixar_documento("999")
    assert downloaded == pdf_expected
    assert counters["auth"] == 1
    assert counters["download"] == 3
    http_client.close()


@pytest.mark.parametrize(
    ("num_attachments", "expected_pages"),
    [
        pytest.param(1, 2, id="single_attachment"),
        pytest.param(3, 6, id="three_attachments_merged_in_order"),
    ],
)
def test_obter_pdf_recurso_processo_merges_attachments(
    num_attachments: int,
    expected_pages: int,
) -> None:
    """Verifica download simples e consolidação em RAM de múltiplos anexos."""
    chunk_pdf = build_pdf_bytes(num_pages=2)

    def handler(request: Request) -> Response:
        if request.url.path.endswith("/documento/listar/123"):
            docs = [
                {
                    "idProtocolo": f"doc-{i + 1}",
                    "atributos": {
                        "tipoDocumento": "E",
                        "mimeType": "application/pdf",
                        "nomeComposto": f"Anexo {i + 1}",
                    },
                }
                for i in range(num_attachments)
            ]
            return json_response({"sucesso": True, "data": {"documentos": docs}})
        if "/documento/baixar/anexo/" in request.url.path:
            return Response(status_code=200, content=chunk_pdf)
        return Response(status_code=404)

    http_client = Client(transport=MockTransport(handler))
    sei = ModWsseiClient(config=_make_config(), http_client=http_client)
    sei.token = "tok"

    ids_csv, merged_bytes = sei.obter_pdf_recurso_processo("123")
    merged_doc = fitz.open(stream=merged_bytes, filetype="pdf")
    try:
        assert len(merged_doc) == expected_pages
    finally:
        merged_doc.close()
        http_client.close()

    expected_ids = ",".join(f"doc-{i + 1}" for i in range(num_attachments))
    assert ids_csv == expected_ids


def test_sei_config_local_preserves_orgao_12_and_allows_http_localhost(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Garante fallback local http://localhost:8000 sem bloqueio SSRF e orgao=12 padrão."""
    for base in (
        "SEI_URL",
        "SEI_API_BASE_URL",
        "SEI_USUARIO",
        "SEI_SERVICE_USER",
        "SEI_SENHA",
        "SEI_SERVICE_PASSWORD",
        "SEI_UNIDADE_ID",
        "SEI_ID_UNIDADE",
        "SEI_ORGAO",
    ):
        for prefix in ("", "SMTR_TRIAGEM__", "SMTR_TRIAGEM_"):
            monkeypatch.delenv(f"{prefix}{base}", raising=False)

    cfg = SeiConfig.from_env(is_local=True)
    assert cfg.base_url == "http://localhost:8000"
    assert cfg.usuario == "usuario_servico"
    assert cfg.id_unidade == "110000001"
    assert cfg.orgao == 12
    core_cfg = cfg.to_core_config()
    assert core_cfg.url == "http://localhost:8000"
    assert core_cfg.orgao == "12"


def test_listar_processos_e_documentos_usam_atributos_numero_e_paginacao_completa() -> None:
    """Valida leitura de atributos.numero do mod-wssei e paginação de documentos (>10)."""

    def handler(request: Request) -> Response:
        if request.url.path.endswith("/processo/listar"):
            return json_response(
                {
                    "sucesso": True,
                    "total": "1",
                    "data": [
                        {
                            "id": "5001",
                            "atributos": {
                                "idProcedimento": "5001",
                                "numero": "000351.000007/2026-99",
                                "unidade": {"idUnidade": "110000715", "sigla": "10_JARI"},
                            },
                        }
                    ],
                }
            )
        if request.url.path.endswith("/documento/listar/5001"):
            start = int(request.url.params.get("start", "0"))
            limit = int(request.url.params.get("limit", "200"))
            assert limit == 200
            if start == 0:
                docs = [
                    {"idProtocolo": f"d-{i}", "atributos": {"tipoDocumento": "X"}} for i in range(12)
                ]
                return json_response({"sucesso": True, "total": 12, "data": docs})
            return json_response({"sucesso": True, "total": 12, "data": []})
        return Response(status_code=404)

    http_client = Client(transport=MockTransport(handler))
    sei = ModWsseiClient(config=_make_config(), http_client=http_client)
    sei.token = "tok"

    processos = sei.listar_processos_caixa()
    assert len(processos) == 1
    assert processos[0]["numero_processo_sei"] == "000351.000007/2026-99"

    docs = sei.listar_documentos_processo("5001")
    http_client.close()
    assert len(docs) == 12
    assert docs[0]["id_documento_sei"] == "d-0"
    assert docs[-1]["id_documento_sei"] == "d-11"


def test_fallback_backend_works_without_external_sei_client(monkeypatch: pytest.MonkeyPatch) -> None:
    """Garante paridade completa no FallbackSeiSync (cluster sem sei-client-smtr privado)."""
    pdf_expected = build_pdf_bytes(num_pages=1, text_prefix="Fallback PDF")
    counters = {"auth": 0, "download": 0}
    seen_usuario_params: list[str | None] = []

    def handler(request: Request) -> Response:
        if request.url.path.endswith("/autenticar"):
            counters["auth"] += 1
            return json_response(
                {
                    "sucesso": True,
                    "data": {
                        "token": f"tok-fb-{counters['auth']}",
                        "loginData": {"sigla": "BOT_FB", "nome": "Bot Fallback"},
                    },
                }
            )
        if request.url.path.endswith("/processo/listar"):
            seen_usuario_params.append(request.url.params.get("usuario"))
            return json_response(
                {
                    "sucesso": True,
                    "total": "1",
                    "data": [
                        {
                            "id": "9001",
                            "dataGeracao": "10/03/2026",
                            "atributos": {
                                "idProcedimento": "9001",
                                "numero": "000351.000999/2026-11",
                                "marcador": [{"nome": "JULGADO #00FF00"}],
                                "dadosAbertura": {
                                    "unidades": [{"id": "1100001"}],
                                    "lista": [{"sigla": "10_JARI (atribuído a maria.relatora)"}],
                                },
                            },
                        }
                    ],
                }
            )
        if request.url.path.endswith("/documento/listar/9001"):
            start = int(request.url.params.get("start", "0"))
            assert request.url.params.get("limit") == "200"
            if start == 0:
                docs = [{"idProtocolo": f"doc-{i}", "atributos": {"tipoDocumento": "X"}} for i in range(11)]
                return json_response({"sucesso": True, "total": 11, "data": docs})
            return json_response({"sucesso": True, "total": 11, "data": []})
        if request.url.path.endswith("/documento/baixar/anexo/doc-0"):
            counters["download"] += 1
            if counters["download"] == 1:
                return Response(status_code=401)
            if counters["download"] == 2:
                return Response(status_code=504)
            return Response(status_code=200, content=pdf_expected)
        return Response(status_code=404)

    monkeypatch.setattr("pipelines.rj_smtr__triagem_recursos._sei_sleep_noop", lambda _s: None, raising=False)
    http_client = Client(transport=MockTransport(handler))
    sei = ModWsseiClient(config=_make_config(), http_client=http_client, usar_cliente_externo=False)
    assert sei._usando_externo is False

    token = sei.autenticar()
    assert token == "tok-fb-1"
    assert sei.sigla_usuario == "BOT_FB"
    assert sei.nome_usuario == "Bot Fallback"

    processos = sei.listar_processos_caixa(usuario_id="42")
    assert seen_usuario_params == ["42"]
    assert processos[0]["numero_processo_sei"] == "000351.000999/2026-11"
    assert processos[0]["usuario_atribuido_sigla"] == "maria.relatora"
    assert processos[0]["marcador"] == "JULGADO #00FF00"

    docs = sei.listar_documentos_processo("9001")
    assert len(docs) == 11

    sei.token = "tok-expirado"
    downloaded = sei.baixar_documento("doc-0")
    http_client.close()
    assert downloaded == pdf_expected
    assert counters["auth"] == 2
    assert counters["download"] == 3


def test_garantir_nivel_logs_pipeline_habilita_info_nos_submodulos() -> None:
    """Verifica que o logger raiz do pacote e submódulos emitem INFO conforme STYLEGUIDE."""
    pkg_logger = get_logger("pipelines.rj_smtr__triagem_recursos")
    pkg_logger.setLevel(0)
    garantir_nivel_logs_pipeline()

    sub_logger = get_logger("pipelines.rj_smtr__triagem_recursos.helpers.db_repository")
    assert pkg_logger.level == INFO
    assert sub_logger.getEffectiveLevel() == INFO


