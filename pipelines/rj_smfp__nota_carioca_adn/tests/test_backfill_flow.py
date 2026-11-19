"""Flow ``rj_smfp__nota_carioca_adn_backfill`` rodando de verdade (API temporária do Prefect) com a fonte trocada."""

import logging
from collections.abc import Iterator

import pytest
from prefect.testing.utilities import prefect_test_harness

from pipelines.rj_smfp__nota_carioca_adn import flow_backfill as flow_module
from pipelines.rj_smfp__nota_carioca_adn import tasks as tasks_module
from pipelines.rj_smfp__nota_carioca_adn.tests.fakes import (
    ArmazenamentoMemoria,
    FonteMemoria,
    IndiceMemoria,
    RelogioFalso,
)
from pipelines.rj_smfp__nota_carioca_adn.utils.backfill import ServicosBackfill


@pytest.fixture(scope="module", autouse=True)
def prefect_api() -> Iterator[None]:
    with prefect_test_harness():
        yield


def test_backfill_flow_fora_da_janela_termina_sem_ler_o_oracle(
    monkeypatch: pytest.MonkeyPatch, relogio: RelogioFalso, caplog: pytest.LogCaptureFixture
) -> None:
    # Given: 09:00 em São Paulo (a fixture ``relogio`` marca 12:00 UTC) e uma fonte que registra qualquer leitura
    fonte, indice = FonteMemoria([]), IndiceMemoria()
    monkeypatch.setattr(flow_module, "inject_bd_credentials_task", lambda **_: None)
    monkeypatch.setattr(
        tasks_module,
        "construir_servicos_backfill",
        lambda _parametros: ServicosBackfill(fonte, ArmazenamentoMemoria(), indice, relogio),
    )
    caplog.set_level(logging.INFO)

    # When
    flow_module.rj_smfp__nota_carioca_adn_backfill(nsu_inicio=1, nsu_fim=1000, ambiente="prod")

    # Then
    assert (fonte.leituras, fonte.consultas_de_nsus) == ([], [])
    assert indice.linhas == []
    assert "fora_da_janela" in caplog.text
