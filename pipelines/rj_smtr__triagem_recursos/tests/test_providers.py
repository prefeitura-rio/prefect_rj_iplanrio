from concurrent.futures import ThreadPoolExecutor

import pytest

from pipelines.rj_smtr__triagem_recursos.helpers.providers.openai_client import OpenAILLMClient
from pipelines.rj_smtr__triagem_recursos.helpers.triage import limpar_cache_cliente_llm, obter_cliente_llm


@pytest.mark.parametrize(
    ("raw_base_url", "expected_base_url"),
    [
        pytest.param("https://bifrost.iplan.dados.rio", "https://bifrost.iplan.dados.rio/openai/v1", id="root_domain"),
        pytest.param(
            "https://bifrost.iplan.dados.rio/openai/v1",
            "https://bifrost.iplan.dados.rio/openai/v1",
            id="already_prefixed",
        ),
    ],
)
def test_openai_client_normalizes_bifrost_base_url(
    monkeypatch: pytest.MonkeyPatch,
    raw_base_url: str,
    expected_base_url: str,
) -> None:
    """Garante normalização correta do sufixo /openai/v1 na URL do Bifrost."""
    monkeypatch.setenv("BIFROST_VIRTUAL_KEY", "test-key")
    monkeypatch.setenv("BIFROST_BASE_URL", raw_base_url)

    client = OpenAILLMClient(default_model="vertex/gemini-3.7-flash")
    try:
        assert client.base_url == expected_base_url
        assert not client.http_client.is_closed
    finally:
        client.close()


def test_openai_client_requires_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exige pelo menos uma chave de API configurada no ambiente."""
    for env_key in ("BIFROST_VIRTUAL_KEY", "BIFROST_API_KEY", "OPENAI_API_KEY"):
        monkeypatch.delenv(env_key, raising=False)

    with pytest.raises(ValueError, match="Chave de API ausente"):
        OpenAILLMClient()


def _get_cached_client(model_name: str) -> object:
    """Busca cliente LLM em cache para o modelo informado."""
    return obter_cliente_llm(model_name)


def test_obter_cliente_llm_caches_and_reuses_across_threads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verifica singleton thread-safe por modelo e fechamento limpo do cache."""
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("BIFROST_VIRTUAL_KEY", "test-key")
    monkeypatch.setenv("BIFROST_BASE_URL", "https://bifrost.iplan.dados.rio/openai/v1")

    with ThreadPoolExecutor(max_workers=6) as pool:
        clients = list(pool.map(_get_cached_client, ["vertex/gemini-3.7-flash"] * 6))

    first = clients[0]
    assert all(client is first for client in clients)
    assert isinstance(first, OpenAILLMClient)
    http_pool = first.http_client

    limpar_cache_cliente_llm()
    assert http_pool.is_closed
