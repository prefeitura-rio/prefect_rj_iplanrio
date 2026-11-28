"""Configuração do cliente da API lida do ambiente do container (sem ``env.py``)."""

import pytest

from pipelines.rj_segur__forca_municipal.client import FMApi

VARIAVEIS = (
    "API_FORCA_MUNICIPAL__API_URL",
    "API_FORCA_MUNICIPAL__API_LOGIN",
    "API_FORCA_MUNICIPAL__API_PASSWORD",
    "API_FORCA_MUNICIPAL__USE_PROXY_URL",
    "API_FORCA_MUNICIPAL__PROXY_URL",
)


@pytest.fixture(autouse=True)
def ambiente_limpo(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove as variáveis da API do ambiente antes de cada teste."""
    for nome in VARIAVEIS:
        monkeypatch.delenv(nome, raising=False)


def test_le_url_e_credenciais_do_ambiente(monkeypatch: pytest.MonkeyPatch) -> None:
    """Lê URL, usuário e senha das variáveis de ambiente."""
    monkeypatch.setenv("API_FORCA_MUNICIPAL__API_URL", "https://api.exemplo.test/")
    monkeypatch.setenv("API_FORCA_MUNICIPAL__API_LOGIN", "usuario-sintetico")
    monkeypatch.setenv("API_FORCA_MUNICIPAL__API_PASSWORD", "senha-sintetica")

    api = FMApi()

    assert api._base_url == "https://api.exemplo.test"
    assert api._username == "usuario-sintetico"
    assert api._password == "senha-sintetica"
    assert api._proxy_url is None


def test_sem_variaveis_mantem_os_valores_vazios() -> None:
    """Sem as variáveis, a URL fica vazia e as credenciais ficam ``None``."""
    api = FMApi()

    assert api._base_url == ""
    assert api._username is None
    assert api._password is None
    assert api._proxy_url is None


@pytest.mark.parametrize("valor", ["true", "1", "yes", " TRUE "])
def test_proxy_ligado_usa_proxy_url(monkeypatch: pytest.MonkeyPatch, valor: str) -> None:
    """Com ``USE_PROXY_URL`` verdadeiro, o cliente usa ``PROXY_URL``."""
    monkeypatch.setenv("API_FORCA_MUNICIPAL__USE_PROXY_URL", valor)
    monkeypatch.setenv("API_FORCA_MUNICIPAL__PROXY_URL", "http://proxy.exemplo.test:3128")

    assert FMApi()._proxy_url == "http://proxy.exemplo.test:3128"


@pytest.mark.parametrize("valor", ["false", "0", "no", ""])
def test_proxy_desligado_ignora_proxy_url(monkeypatch: pytest.MonkeyPatch, valor: str) -> None:
    """Com ``USE_PROXY_URL`` falso ou vazio, ``PROXY_URL`` é ignorada."""
    monkeypatch.setenv("API_FORCA_MUNICIPAL__USE_PROXY_URL", valor)
    monkeypatch.setenv("API_FORCA_MUNICIPAL__PROXY_URL", "http://proxy.exemplo.test:3128")

    assert FMApi()._proxy_url is None
