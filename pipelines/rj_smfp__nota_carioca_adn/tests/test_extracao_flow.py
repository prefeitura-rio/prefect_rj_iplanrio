"""Flow ``rj_smfp__nota_carioca_adn_extracao`` rodando de verdade (API temporária do Prefect) contra o dublê do ADN."""

from collections.abc import Callable, Iterator

import pytest
from prefect.testing.utilities import prefect_test_harness

from pipelines.rj_smfp__nota_carioca_adn import flow as flow_module
from pipelines.rj_smfp__nota_carioca_adn import tasks as tasks_module
from pipelines.rj_smfp__nota_carioca_adn.tests.duble_adn import Comportamento, DubleAdn, erro_adn
from pipelines.rj_smfp__nota_carioca_adn.tests.fakes import ArmazenamentoMemoria, IndiceMemoria, RelogioFalso
from pipelines.rj_smfp__nota_carioca_adn.utils.adn_client import AdnClient, AdnConfig
from pipelines.rj_smfp__nota_carioca_adn.utils.adn_errors import CnpjNaoCadastrado
from pipelines.rj_smfp__nota_carioca_adn.utils.extrator import Servicos
from pipelines.rj_smfp__nota_carioca_adn.utils.notify import NotificadorNulo


@pytest.fixture(scope="module", autouse=True)
def prefect_api() -> Iterator[None]:
    with prefect_test_harness():
        yield


@pytest.fixture
def portas_do_flow(
    monkeypatch: pytest.MonkeyPatch,
    config_adn: Callable[..., AdnConfig],
    armazenamento: ArmazenamentoMemoria,
    indice: IndiceMemoria,
    relogio: RelogioFalso,
) -> None:
    """Troca só o que sai do processo (credenciais GCP e clientes de GCS e BigQuery); o ADN é o dublê de verdade."""
    monkeypatch.setattr(flow_module, "inject_bd_credentials_task", lambda **_: None)
    monkeypatch.setattr(
        tasks_module,
        "construir_servicos",
        lambda _parametros: Servicos(
            AdnClient(config_adn(), dormir=lambda _segundos: None), armazenamento, indice, NotificadorNulo(), relogio
        ),
    )


@pytest.mark.usefixtures("portas_do_flow")
def test_extracao_flow_percorre_o_adn_e_deixa_o_indice_completo(duble: DubleAdn, indice: IndiceMemoria) -> None:
    # Given: um ADN de 120 NSUs
    duble.comportamento = Comportamento(total_nsu=120)

    # When: o flow roda com os parâmetros do deployment de staging
    flow_module.rj_smfp__nota_carioca_adn_extracao(
        ambiente="staging", nsu_inicial=0, intervalo_entre_chamadas_s=0, discord_notifications=False
    )

    # Then
    assert indice.nsus() == list(range(1, 121))
    assert indice.ler_cursor("staging") == 120


@pytest.mark.usefixtures("portas_do_flow")
def test_extracao_flow_falha_quando_o_adn_rejeita_o_certificado(duble: DubleAdn, indice: IndiceMemoria) -> None:
    # Given: o ADN rejeita o CNPJ (E2001)
    duble.comportamento.roteiro.append(erro_adn(400, "E2001"))

    # When / Then: o flow falha com a exceção tipada (o run aparece como Failed no Prefect)
    with pytest.raises(CnpjNaoCadastrado):
        flow_module.rj_smfp__nota_carioca_adn_extracao(
            ambiente="staging", nsu_inicial=0, intervalo_entre_chamadas_s=0, discord_notifications=False
        )
    assert indice.linhas == []
