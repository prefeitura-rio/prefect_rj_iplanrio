from collections.abc import Iterator

import httpx
import pytest
from prefect.testing.utilities import prefect_test_harness

from pipelines.rj_smfp__nota_carioca_extracao_xml import flow as flow_module

TOKEN_ENV = "DB_ORACLE_NOTA_FISCAL__NOTA_CARIOCA_ROTINAS_TOKEN"


@pytest.fixture(scope="module", autouse=True)
def prefect_api() -> Iterator[None]:
    """API temporária do Prefect (SQLite): a task e o flow rodam de verdade, sem tocar no Prefect real."""
    with prefect_test_harness():
        yield


class NoNetworkError(AssertionError):
    pass


def test_flow_fails_before_any_call_when_the_token_is_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(TOKEN_ENV, raising=False)

    def no_client(*_args: object, **_kwargs: object) -> httpx.Client:
        raise NoNetworkError("o webservice não pode ser chamado sem token")

    monkeypatch.setattr(httpx, "Client", no_client)

    with pytest.raises(ValueError, match=TOKEN_ENV):
        flow_module.rj_smfp__nota_carioca_extracao_xml(discord_notifications=False)


def test_flow_refuses_a_non_https_url_before_reading_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(TOKEN_ENV, "t")

    with pytest.raises(ValueError, match="https"):
        flow_module.rj_smfp__nota_carioca_extracao_xml(url="http://x/a.asmx", discord_notifications=False)
