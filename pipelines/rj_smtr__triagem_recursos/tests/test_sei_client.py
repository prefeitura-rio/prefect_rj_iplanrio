from concurrent.futures import ThreadPoolExecutor
from threading import Lock

from httpx import Client, MockTransport, Request, Response
import pymupdf as fitz
import pytest

from pipelines.rj_smtr__triagem_recursos.tests.conftest import build_pdf_bytes, json_response
from pipelines.rj_smtr__triagem_recursos.utils.sei_client import ModWsseiClient, SeiConfig


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
                    "atributos": {"tipoDocumento": "E", "mimeType": "application/pdf", "nomeComposto": f"Anexo {i + 1}"},
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
