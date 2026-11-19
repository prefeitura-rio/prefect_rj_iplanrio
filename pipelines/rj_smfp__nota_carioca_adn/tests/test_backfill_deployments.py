"""``prefect.yaml`` do backfill: agenda inativa até o gate 38, uma execução por vez e a janela da DBA."""

from pathlib import Path

import pytest
import yaml

PREFECT_YAML = Path(__file__).parent.parent / "prefect.yaml"
NOME = "rj-smfp--nota-carioca-adn-backfill--prod"


@pytest.fixture(scope="module")
def deployment() -> dict[str, object]:
    documento = yaml.safe_load(PREFECT_YAML.read_text())
    return next(item for item in documento["deployments"] if item["name"] == NOME)


def test_backfill_deployment_tem_agenda_inativa_e_roda_uma_execucao_por_vez(deployment: dict[str, object]) -> None:
    assert all(agenda["active"] is False for agenda in deployment["schedules"])  # type: ignore[attr-defined, index]
    assert deployment["concurrency_limit"]["limit"] == 1  # type: ignore[index]


def test_backfill_deployment_padrao_e_20000_linhas_por_minuto_na_janela_20h_as_6h(
    deployment: dict[str, object],
) -> None:
    parametros = deployment["parameters"]
    assert parametros["linhas_por_minuto"] == 20000  # type: ignore[index]
    assert parametros["janela_horaria"] == "20:00-06:00"  # type: ignore[index]
    assert parametros["ambiente"] == "prod"  # type: ignore[index]
    assert "nsu_inicio" not in parametros  # type: ignore[operator]
    assert "nsu_fim" not in parametros  # type: ignore[operator]
