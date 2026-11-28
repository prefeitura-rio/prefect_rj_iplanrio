"""Configuração do envio de e-mails lida do ambiente do container (sem ``env.py``)."""

import importlib
from collections.abc import Iterator
from types import ModuleType

import pytest

from pipelines.rj_pic__disparos_email import engine, flow, tasks

PREFIXO = "PIC_DISPAROS_EMAIL_"


CONSTANTES = (
    "DATA_RELAY_URL",
    "DATA_RELAY_API_KEY",
    "DATA_RELAY_FROM_ADDRESS",
    "DATA_RELAY_USE_GMAIL_API",
    "DATA_RELAY_IS_HTML_BODY",
    "MAX_RETRIES",
    "RETRY_DELAY",
    "THROTTLE_DELAY",
)


def configuracao(modulo: ModuleType) -> dict[str, object]:
    """Lê as constantes de configuração do módulo ``engine``.

    :param modulo: O módulo ``engine`` carregado.
    :returns: Nome e valor de cada constante de configuração.
    """
    return {nome: getattr(modulo, nome) for nome in CONSTANTES}


def recarregar(monkeypatch: pytest.MonkeyPatch, **variaveis: str) -> ModuleType:
    """Recarrega ``engine`` só com as variáveis informadas no ambiente.

    :param monkeypatch: Fixture do pytest que isola o ambiente do teste.
    :param variaveis: Variáveis sem o prefixo ``PIC_DISPAROS_EMAIL_``.
    :returns: O módulo ``engine`` recarregado.
    """
    for nome in CONSTANTES:
        monkeypatch.delenv(PREFIXO + nome, raising=False)
    for nome, valor in variaveis.items():
        monkeypatch.setenv(PREFIXO + nome, valor)
    return importlib.reload(engine)


@pytest.fixture(autouse=True)
def restaura_modulos() -> Iterator[None]:
    """Devolve os módulos ao estado do ambiente real depois de cada teste."""
    yield
    for modulo in (engine, tasks, flow):
        importlib.reload(modulo)


def test_engine_sem_variaveis_usa_os_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sem variáveis, valem os mesmos defaults do antigo ``env.py``."""
    modulo = recarregar(monkeypatch)

    assert configuracao(modulo) == {
        "DATA_RELAY_URL": "https://staging.data-relay.dados.rio/data/mailman",
        "DATA_RELAY_API_KEY": None,
        "DATA_RELAY_FROM_ADDRESS": None,
        "DATA_RELAY_USE_GMAIL_API": True,
        "DATA_RELAY_IS_HTML_BODY": True,
        "MAX_RETRIES": 3,
        "RETRY_DELAY": 5,
        "THROTTLE_DELAY": 1.0,
    }


def test_engine_le_as_variaveis_do_ambiente(monkeypatch: pytest.MonkeyPatch) -> None:
    """Cada valor vem da variável de mesmo nome do antigo ``env.py``."""
    modulo = recarregar(
        monkeypatch,
        DATA_RELAY_URL="https://relay.exemplo.test/mailman",
        DATA_RELAY_API_KEY="chave-sintetica",
        DATA_RELAY_FROM_ADDRESS="remetente@exemplo.test",
        DATA_RELAY_USE_GMAIL_API="FALSE",
        DATA_RELAY_IS_HTML_BODY="false",
        MAX_RETRIES="7",
        RETRY_DELAY="2",
        THROTTLE_DELAY="0.5",
    )

    assert configuracao(modulo) == {
        "DATA_RELAY_URL": "https://relay.exemplo.test/mailman",
        "DATA_RELAY_API_KEY": "chave-sintetica",
        "DATA_RELAY_FROM_ADDRESS": "remetente@exemplo.test",
        "DATA_RELAY_USE_GMAIL_API": False,
        "DATA_RELAY_IS_HTML_BODY": False,
        "MAX_RETRIES": 7,
        "RETRY_DELAY": 2,
        "THROTTLE_DELAY": 0.5,
    }


def test_sender_sem_chave_continua_recusando(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sem a chave da API no ambiente, o ``EmailSender`` recusa iniciar."""
    modulo = recarregar(monkeypatch)

    with pytest.raises(ValueError, match="PIC_DISPAROS_EMAIL_DATA_RELAY_API_KEY"):
        modulo.EmailSender()


def test_tasks_le_filtro_de_emails(monkeypatch: pytest.MonkeyPatch) -> None:
    """``FILTER_EMAILS`` vem do ambiente e tem string vazia como default."""
    monkeypatch.delenv(PREFIXO + "FILTER_EMAILS", raising=False)
    assert importlib.reload(tasks).FILTER_EMAILS == ""

    monkeypatch.setenv(PREFIXO + "FILTER_EMAILS", "a@exemplo.test,b@exemplo.test")
    assert importlib.reload(tasks).FILTER_EMAILS == "a@exemplo.test,b@exemplo.test"


def test_flow_le_tabela_do_bigquery(monkeypatch: pytest.MonkeyPatch) -> None:
    """Projeto, dataset e tabela do flow vêm do ambiente."""
    monkeypatch.setenv(PREFIXO + "BIGQUERY_PROJECT_ID", "projeto-sintetico")
    monkeypatch.setenv(PREFIXO + "BIGQUERY_DATASET_ID", "dataset_sintetico")
    monkeypatch.setenv(PREFIXO + "BIGQUERY_TABLE_ID", "tabela_sintetica")

    modulo = importlib.reload(flow)

    assert modulo.BIGQUERY_PROJECT_ID == "projeto-sintetico"
    assert modulo.BIGQUERY_DATASET_ID == "dataset_sintetico"
    assert modulo.BIGQUERY_TABLE_ID == "tabela_sintetica"
