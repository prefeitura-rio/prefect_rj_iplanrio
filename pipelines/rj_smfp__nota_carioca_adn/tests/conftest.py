from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from pipelines.rj_smfp__nota_carioca_adn.tests.certificados import Pki, gerar_pki
from pipelines.rj_smfp__nota_carioca_adn.tests.duble_adn import Comportamento, DubleAdn
from pipelines.rj_smfp__nota_carioca_adn.utils.adn_client import AdnClient, AdnConfig


class Dorminhoco:
    """Substitui ``time.sleep`` e guarda as esperas pedidas; nenhum teste espera de verdade."""

    def __init__(self) -> None:
        self.esperas: list[float] = []

    def __call__(self, segundos: float) -> None:
        self.esperas.append(segundos)


@pytest.fixture(scope="session")
def pki(tmp_path_factory: pytest.TempPathFactory) -> Pki:
    return gerar_pki(tmp_path_factory.mktemp("pki"))


@pytest.fixture
def duble(pki: Pki) -> Iterator[DubleAdn]:
    with DubleAdn(pki, Comportamento()) as servidor:
        yield servidor


@pytest.fixture
def dormir() -> Dorminhoco:
    return Dorminhoco()


@pytest.fixture
def config_adn(pki: Pki, duble: DubleAdn) -> Callable[..., AdnConfig]:
    """Fábrica de configuração apontada para o dublê; ``**alteracoes`` troca qualquer campo."""

    def fabricar(**alteracoes: object) -> AdnConfig:
        campos: dict[str, object] = {
            "base_url": duble.url,
            "pfx_base64": pki.pfx_base64,
            "pfx_senha": pki.senha,
            "ca_bundle": str(pki.ca_pem),
            "cursor_inclusivo": duble.comportamento.cursor_inclusivo,
            "tamanho_lote_esperado": duble.comportamento.lote_max,
        }
        return AdnConfig(**{**campos, **alteracoes})  # type: ignore[arg-type]

    return fabricar


@pytest.fixture
def cliente(config_adn: Callable[..., AdnConfig], dormir: Dorminhoco) -> Iterator[AdnClient]:
    with AdnClient(config_adn(), dormir=dormir) as adn:
        yield adn


@pytest.fixture(autouse=True)
def _tmpdir_isolado(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Arquivos temporários de cada teste ficam no ``tmp_path`` do teste, para os testes verem o que sobrou."""
    pasta = tmp_path / "tmp"
    pasta.mkdir()
    monkeypatch.setenv("TMPDIR", str(pasta))
    monkeypatch.setattr("tempfile.tempdir", str(pasta))
