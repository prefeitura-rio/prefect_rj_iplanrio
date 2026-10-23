from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from pipelines.rj_smfp__nota_carioca_adn.utils.extrator import Servicos, executar_extracao
from pipelines.rj_smfp__nota_carioca_adn.utils.modelos import ConfigExtracao, ResultadoExtracao

from pipelines.rj_smfp__nota_carioca_adn.tests.certificados import Pki, gerar_pki
from pipelines.rj_smfp__nota_carioca_adn.tests.duble_adn import Comportamento, DubleAdn
from pipelines.rj_smfp__nota_carioca_adn.tests.fakes import (
    ArmazenamentoMemoria,
    IndiceMemoria,
    NotificadorGravador,
    RelogioFalso,
)
from pipelines.rj_smfp__nota_carioca_adn.utils import discord
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


def _sem_secret_block(nome: str) -> str:
    raise ValueError(f"Secret block {nome} indisponível nos testes")


@pytest.fixture(autouse=True)
def _sem_discord_de_verdade(monkeypatch: pytest.MonkeyPatch) -> None:
    """Nenhum teste fala com o Discord de verdade, mesmo com o webhook no ambiente de quem roda."""
    monkeypatch.delenv("DISCORD_WEBHOOK_URL_NOTA_CARIOCA", raising=False)
    monkeypatch.delenv("DISCORD_WEBHOOK_URL_NOTA_CARIOCA_STAGING", raising=False)
    monkeypatch.setattr(discord, "_load_secret_block", _sem_secret_block)


@pytest.fixture
def armazenamento() -> ArmazenamentoMemoria:
    return ArmazenamentoMemoria()


@pytest.fixture
def indice() -> IndiceMemoria:
    return IndiceMemoria()


@pytest.fixture
def relogio() -> RelogioFalso:
    return RelogioFalso()


@pytest.fixture
def notificador() -> NotificadorGravador:
    return NotificadorGravador()


@pytest.fixture
def extrair(  # noqa: PLR0913, PLR0917 - a fixture reúne as portas do extrator
    duble: DubleAdn,
    config_adn: Callable[..., AdnConfig],
    dormir: Dorminhoco,
    armazenamento: ArmazenamentoMemoria,
    indice: IndiceMemoria,
    relogio: RelogioFalso,
    notificador: NotificadorGravador,
) -> Callable[..., ResultadoExtracao]:
    """Roda um run do extrator contra o dublê; ``comportamento`` troca o dublê e o resto vai para a configuração."""

    def executar(comportamento: Comportamento | None = None, **alteracoes: object) -> ResultadoExtracao:
        if comportamento is not None:
            duble.comportamento = comportamento
        campos: dict[str, object] = {"ambiente": "staging", "intervalo_entre_chamadas_s": 0.0, "nsu_inicial": 0}
        configuracao = ConfigExtracao(**{**campos, **alteracoes})  # type: ignore[arg-type]
        with AdnClient(config_adn(), dormir=dormir) as adn:
            return executar_extracao(configuracao, Servicos(adn, armazenamento, indice, notificador, relogio))

    return executar
