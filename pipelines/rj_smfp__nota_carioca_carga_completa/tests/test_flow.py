from collections.abc import Iterator, Sequence
from pathlib import Path

import pytest
import yaml
from prefect import tags
from prefect.testing.utilities import prefect_test_harness

from pipelines.rj_smfp__nota_carioca_carga_completa import flow as flow_module
from pipelines.rj_smfp__nota_carioca_carga_completa import tasks as tasks_module
from pipelines.rj_smfp__nota_carioca_carga_completa.constants import (
    BQ_TO_ORACLE_DEPLOYMENT,
    EXTRACTION_DEPLOYMENT,
    ORACLE_TO_BQ_DEPLOYMENT,
)
from pipelines.rj_smfp__nota_carioca_carga_completa.utils import runs
from pipelines.rj_smfp__nota_carioca_carga_completa.utils.chain import ChainStoppedError, ChildRun
from pipelines.rj_smfp__nota_carioca_carga_completa.utils.discord import Environment


@pytest.fixture(scope="module", autouse=True)
def prefect_api() -> Iterator[None]:
    """API temporária do Prefect (SQLite): as tasks e o flow rodam de verdade, sem tocar no Prefect real."""
    with prefect_test_harness():
        yield


class FakePrefect:
    """Só o que sai do processo: consulta de execuções ativas e disparo de deployments."""

    def __init__(self, outcomes: dict[str, ChildRun]) -> None:
        self.outcomes = outcomes
        self.active: dict[str, list[runs.ActiveRun]] = {}
        self.launched: list[tuple[str, int, Sequence[str]]] = []

    def list_deployment_runs(self, deployment: str) -> list[runs.ActiveRun]:
        return self.active.get(deployment, [])

    def run_and_wait(self, deployment: str, timeout_seconds: int, run_tags: Sequence[str]) -> ChildRun:
        self.launched.append((deployment, timeout_seconds, run_tags))
        return self.outcomes[deployment]


def child(name: str, state_type: str, state_name: str) -> ChildRun:
    return ChildRun(run_id=f"id-{name}", name=name, state_type=state_type, state_name=state_name)


@pytest.fixture
def fake_prefect(monkeypatch: pytest.MonkeyPatch) -> FakePrefect:
    fake = FakePrefect(
        {
            EXTRACTION_DEPLOYMENT: child("xml-run", "COMPLETED", "Completed"),
            ORACLE_TO_BQ_DEPLOYMENT: child("oracle-run", "COMPLETED", "Completed"),
            BQ_TO_ORACLE_DEPLOYMENT: child("bq-run", "COMPLETED", "Completed"),
        }
    )
    monkeypatch.setattr(runs, "list_deployment_runs", fake.list_deployment_runs)
    monkeypatch.setattr(runs, "run_and_wait", fake.run_and_wait)
    monkeypatch.setattr(tasks_module.time, "sleep", lambda _seconds: None)
    return fake


def test_flow_chains_extraction_and_both_loads_with_default_timeouts_and_inherited_tags(
    fake_prefect: FakePrefect,
) -> None:
    # Given: o run do orquestrador foi disparado pelo bot do Discord (tags de auditoria)
    # When: o flow roda com os parâmetros padrão
    with tags("disparo:discord", "discord-user:42", "environment:prod"):
        flow_module.rj_smfp__nota_carioca_carga_completa(discord_notifications=False)

    # Then: dispara a extração XML (9 h), Oracle → BigQuery (4 h) e BigQuery → Oracle (8 h), com as tags herdadas
    audit_tags = {"disparo:carga-completa", "disparo:discord", "discord-user:42"}
    assert [(deployment, timeout, set(run_tags)) for deployment, timeout, run_tags in fake_prefect.launched] == [
        (EXTRACTION_DEPLOYMENT, 9 * 3600, audit_tags),
        (ORACLE_TO_BQ_DEPLOYMENT, 4 * 3600, audit_tags),
        (BQ_TO_ORACLE_DEPLOYMENT, 8 * 3600, audit_tags),
    ]


def test_flow_can_skip_the_extraction(fake_prefect: FakePrefect) -> None:
    flow_module.rj_smfp__nota_carioca_carga_completa(incluir_extracao_xml=False, discord_notifications=False)

    assert [launched[0] for launched in fake_prefect.launched] == [ORACLE_TO_BQ_DEPLOYMENT, BQ_TO_ORACLE_DEPLOYMENT]


def test_flow_runs_no_load_when_the_extraction_fails(fake_prefect: FakePrefect) -> None:
    fake_prefect.outcomes[EXTRACTION_DEPLOYMENT] = child("xml-run", "FAILED", "Failed")

    with pytest.raises(ChainStoppedError, match="Extração XML terminou em Failed"):
        flow_module.rj_smfp__nota_carioca_carga_completa(discord_notifications=False)
    assert [launched[0] for launched in fake_prefect.launched] == [EXTRACTION_DEPLOYMENT]


def test_flow_fails_without_second_when_first_fails(fake_prefect: FakePrefect) -> None:
    # Given: o primeiro deployment de carga termina em Failed
    fake_prefect.outcomes[ORACLE_TO_BQ_DEPLOYMENT] = child("oracle-run", "FAILED", "Failed")

    # When / Then: o flow falha e o segundo deployment não é disparado
    with pytest.raises(ChainStoppedError, match="Oracle → BigQuery terminou em Failed"):
        flow_module.rj_smfp__nota_carioca_carga_completa(discord_notifications=False)
    assert [launched[0] for launched in fake_prefect.launched] == [EXTRACTION_DEPLOYMENT, ORACLE_TO_BQ_DEPLOYMENT]


def test_flow_refuses_to_start_when_a_target_deployment_has_an_active_run(fake_prefect: FakePrefect) -> None:
    # Given: alguém disparou o BigQuery → Oracle à mão pela UI e ele está rodando
    fake_prefect.active[BQ_TO_ORACLE_DEPLOYMENT] = [
        runs.ActiveRun("id-manual", "manual-run", "RUNNING", "Running", expected_start_time=None)
    ]

    # When / Then: o flow falha pela trava, nomeando a execução, e nada é disparado
    with pytest.raises(ChainStoppedError, match=r"não iniciada.*manual-run \(Running\)"):
        flow_module.rj_smfp__nota_carioca_carga_completa(discord_notifications=False)
    assert fake_prefect.launched == []


@pytest.mark.usefixtures("fake_prefect")
@pytest.mark.parametrize(
    ("deployment_name", "expected"),
    [
        ("rj-smfp--nota-carioca-carga-completa--staging", Environment.STAGING),
        ("rj-smfp--nota-carioca-carga-completa--prod", Environment.PROD),
    ],
)
def test_flow_notifies_in_the_environment_of_its_deployment(
    monkeypatch: pytest.MonkeyPatch, deployment_name: str, expected: Environment
) -> None:
    # Given: o runtime do Prefect informa o nome do deployment deste run
    monkeypatch.setenv("PREFECT__RUNTIME__DEPLOYMENT__NAME", deployment_name)
    created: list[tuple[bool, Environment, tuple[str, ...]]] = []

    def make_notifier(enabled: bool, environment: Environment, mention_role_ids: tuple[str, ...] = ()) -> object:
        created.append((enabled, environment, tuple(mention_role_ids)))
        return lambda _text: None

    monkeypatch.setattr(flow_module, "make_notifier", make_notifier)

    # When: o flow roda
    flow_module.rj_smfp__nota_carioca_carga_completa(discord_alert_role_ids=["111", "222"])

    # Then: o aviso e o alerta (com os cargos) nascem com o ambiente derivado do nome do deployment
    assert created == [(True, expected, ()), (True, expected, ("111", "222"))]


PREFECT_YAML = Path(__file__).resolve().parents[1] / "prefect.yaml"


def deployments() -> dict[str, dict[str, object]]:
    data = yaml.safe_load(PREFECT_YAML.read_text(encoding="utf-8"))
    return {item["name"]: item for item in data["deployments"]}


def test_prod_runs_every_day_at_17h_in_rio_and_staging_has_no_schedule() -> None:
    found = deployments()

    assert found["rj-smfp--nota-carioca-carga-completa--prod"]["schedules"] == [
        {"cron": "0 17 * * *", "timezone": "America/Sao_Paulo", "active": True}
    ]
    assert "schedules" not in found["rj-smfp--nota-carioca-carga-completa--staging"]


def test_staging_chains_the_staging_extraction() -> None:
    parameters = deployments()["rj-smfp--nota-carioca-carga-completa--staging"]["parameters"]

    assert parameters["extracao_xml_deployment"] == (
        "rj-smfp--nota-carioca-extracao-xml/rj-smfp--nota-carioca-extracao-xml--staging"
    )


def test_both_environments_mention_the_same_alert_roles() -> None:
    found = deployments()

    staging = found["rj-smfp--nota-carioca-carga-completa--staging"]["parameters"]["discord_alert_role_ids"]
    prod = found["rj-smfp--nota-carioca-carga-completa--prod"]["parameters"]["discord_alert_role_ids"]
    assert staging == prod == ["1558828140166647901", "903644455582695494"]
