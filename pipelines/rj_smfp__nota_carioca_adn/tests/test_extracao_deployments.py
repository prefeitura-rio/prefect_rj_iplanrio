"""``prefect.yaml``: o que o CI de deploy lê — nomes, agenda inativa até o gate 38 e concorrência 1."""

from pathlib import Path

import pytest
import yaml

PREFECT_YAML = Path(__file__).parent.parent / "prefect.yaml"
EXTRACAO = ("rj-smfp--nota-carioca-adn-extracao--staging", "rj-smfp--nota-carioca-adn-extracao--prod")


@pytest.fixture(scope="module")
def deployments() -> dict[str, dict[str, object]]:
    documento = yaml.safe_load(PREFECT_YAML.read_text())
    return {deployment["name"]: deployment for deployment in documento["deployments"]}


def test_extracao_deployments_de_staging_e_de_prod_existem(deployments: dict[str, dict[str, object]]) -> None:
    assert {nome for nome in deployments if "extracao" in nome} == set(EXTRACAO)


def test_extracao_nenhuma_agenda_esta_ativa_ate_o_gate_38(deployments: dict[str, dict[str, object]]) -> None:
    ativas = [
        (nome, agenda)
        for nome, deployment in deployments.items()
        for agenda in deployment.get("schedules", [])  # type: ignore[attr-defined]
        if agenda["active"] is not False  # type: ignore[index]
    ]
    assert ativas == []


@pytest.mark.parametrize("nome", EXTRACAO)
def test_extracao_roda_um_flow_run_por_vez_e_ha_agenda_de_60_minutos(
    deployments: dict[str, dict[str, object]], nome: str
) -> None:
    deployment = deployments[nome]
    assert deployment["concurrency_limit"]["limit"] == 1  # type: ignore[index]
    assert [agenda["interval"] for agenda in deployment["schedules"]] == [3600]  # type: ignore[attr-defined, index]


def test_extracao_ambiente_do_deployment_confere_com_o_parametro(deployments: dict[str, dict[str, object]]) -> None:
    assert deployments[EXTRACAO[0]]["parameters"]["ambiente"] == "staging"  # type: ignore[index]
    assert deployments[EXTRACAO[1]]["parameters"]["ambiente"] == "prod"  # type: ignore[index]


DATASET_POR_DEPLOYMENT = {
    "rj-smfp--nota-carioca-adn-extracao--staging": "raw_nfse_nacional_staging",
    "rj-smfp--nota-carioca-adn-extracao--prod": "raw_nfse_nacional",
    "rj-smfp--nota-carioca-adn-backfill--prod": "raw_nfse_nacional",
}


@pytest.mark.parametrize(("nome", "dataset"), DATASET_POR_DEPLOYMENT.items())
def test_dataset_do_deployment_segue_a_convencao_do_ambiente(
    deployments: dict[str, dict[str, object]], nome: str, dataset: str
) -> None:
    assert deployments[nome]["parameters"]["dataset_id"] == dataset  # type: ignore[index]
