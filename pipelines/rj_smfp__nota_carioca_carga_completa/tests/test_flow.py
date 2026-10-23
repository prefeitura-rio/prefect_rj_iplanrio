from collections.abc import Iterator, Mapping, Sequence

import pytest
from prefect import tags
from prefect.testing.utilities import prefect_test_harness

from pipelines.rj_smfp__nota_carioca_carga_completa import flow as flow_module
from pipelines.rj_smfp__nota_carioca_carga_completa import tasks as tasks_module
from pipelines.rj_smfp__nota_carioca_carga_completa.constants import (
    BQ_TO_ORACLE_DEPLOYMENT,
    BQ_TO_POSTGRES_DEPLOYMENT,
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
        self.launched_parameters: list[Mapping[str, object]] = []

    def list_deployment_runs(self, deployment: str) -> list[runs.ActiveRun]:
        return self.active.get(deployment, [])

    def run_and_wait(
        self,
        deployment: str,
        timeout_seconds: int,
        run_tags: Sequence[str],
        parameters: Mapping[str, object] | None = None,
    ) -> ChildRun:
        self.launched.append((deployment, timeout_seconds, run_tags))
        self.launched_parameters.append(parameters or {})
        return self.outcomes[deployment]


def child(name: str, state_type: str, state_name: str) -> ChildRun:
    return ChildRun(run_id=f"id-{name}", name=name, state_type=state_type, state_name=state_name)


@pytest.fixture
def fake_prefect(monkeypatch: pytest.MonkeyPatch) -> FakePrefect:
    fake = FakePrefect(
        {
            ORACLE_TO_BQ_DEPLOYMENT: child("oracle-run", "COMPLETED", "Completed"),
            BQ_TO_ORACLE_DEPLOYMENT: child("bq-run", "COMPLETED", "Completed"),
            BQ_TO_POSTGRES_DEPLOYMENT: child("pg-run", "COMPLETED", "Completed"),
        }
    )
    monkeypatch.setattr(runs, "list_deployment_runs", fake.list_deployment_runs)
    monkeypatch.setattr(runs, "run_and_wait", fake.run_and_wait)
    monkeypatch.setattr(tasks_module.time, "sleep", lambda _seconds: None)
    return fake


def test_flow_chains_both_deployments_with_default_timeouts_and_inherited_tags(fake_prefect: FakePrefect) -> None:
    # Given: o run do orquestrador foi disparado pelo bot do Discord (tags de auditoria)
    # When: o flow roda com os parâmetros padrão
    with tags("disparo:discord", "discord-user:42", "environment:prod"):
        flow_module.rj_smfp__nota_carioca_carga_completa(discord_notifications=False)

    # Then: dispara Oracle → BigQuery (4 h) e BigQuery → Oracle (8 h), ambos com as tags herdadas
    audit_tags = {"disparo:carga-completa", "disparo:discord", "discord-user:42"}
    assert [(deployment, timeout, set(run_tags)) for deployment, timeout, run_tags in fake_prefect.launched] == [
        (ORACLE_TO_BQ_DEPLOYMENT, 4 * 3600, audit_tags),
        (BQ_TO_ORACLE_DEPLOYMENT, 8 * 3600, audit_tags),
    ]


def test_flow_fails_without_second_when_first_fails(fake_prefect: FakePrefect) -> None:
    # Given: o primeiro deployment termina em Failed
    fake_prefect.outcomes[ORACLE_TO_BQ_DEPLOYMENT] = child("oracle-run", "FAILED", "Failed")

    # When / Then: o flow falha e o segundo deployment não é disparado
    with pytest.raises(ChainStoppedError, match="Oracle → BigQuery terminou em Failed"):
        flow_module.rj_smfp__nota_carioca_carga_completa(discord_notifications=False)
    assert [launched[0] for launched in fake_prefect.launched] == [ORACLE_TO_BQ_DEPLOYMENT]


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
    created: list[tuple[bool, Environment]] = []

    def make_notifier(enabled: bool, environment: Environment) -> object:
        created.append((enabled, environment))
        return lambda _text: None

    monkeypatch.setattr(flow_module, "make_notifier", make_notifier)

    # When: o flow roda
    flow_module.rj_smfp__nota_carioca_carga_completa()

    # Then: o notificador nasce com o ambiente derivado do nome do deployment
    assert created == [(True, expected)]


def test_flow_does_not_ask_bq_to_oracle_to_preserve_the_export_by_default(fake_prefect: FakePrefect) -> None:
    # Given / When: o flow roda com os padrões (incluir_postgres desligado)
    flow_module.rj_smfp__nota_carioca_carga_completa(discord_notifications=False)

    # Then: só os dois filhos de sempre, e o bq_to_oracle recebe a preservação desligada
    assert [launched[0] for launched in fake_prefect.launched] == [ORACLE_TO_BQ_DEPLOYMENT, BQ_TO_ORACLE_DEPLOYMENT]
    assert fake_prefect.launched_parameters == [{}, {"preservar_export_para_postgres": False}]


def test_flow_with_incluir_postgres_preserves_the_export_and_loads_postgres_from_its_manifest(
    fake_prefect: FakePrefect,
) -> None:
    # When: a carga completa inclui o Postgres
    flow_module.rj_smfp__nota_carioca_carga_completa(incluir_postgres=True, discord_notifications=False)

    # Then: o bq_to_oracle preserva o export e o Postgres carrega o manifesto do run dele (4 h de limite)
    assert [(launched[0], launched[1]) for launched in fake_prefect.launched] == [
        (ORACLE_TO_BQ_DEPLOYMENT, 4 * 3600),
        (BQ_TO_ORACLE_DEPLOYMENT, 8 * 3600),
        (BQ_TO_POSTGRES_DEPLOYMENT, 4 * 3600),
    ]
    assert fake_prefect.launched_parameters == [
        {},
        {"preservar_export_para_postgres": True},
        {"manifesto_uri": "gs://rj-iplanrio-dia-bq-to-oracle/nota_carioca/id-bq-run/manifesto.json"},
    ]


def test_flow_staging_parameters_point_to_the_staging_deployment_and_dataset(fake_prefect: FakePrefect) -> None:
    fake_prefect.outcomes["flow/staging"] = child("pg-run", "COMPLETED", "Completed")

    flow_module.rj_smfp__nota_carioca_carga_completa(
        incluir_postgres=True,
        bq_to_postgres_deployment="flow/staging",
        export_bucket="outro-bucket",
        export_dataset_id="nota_carioca_staging",
        discord_notifications=False,
    )

    assert fake_prefect.launched[-1][0] == "flow/staging"
    assert fake_prefect.launched_parameters[-1] == {
        "manifesto_uri": "gs://outro-bucket/nota_carioca_staging/id-bq-run/manifesto.json"
    }


def test_flow_does_not_load_postgres_when_bq_to_oracle_fails(fake_prefect: FakePrefect) -> None:
    fake_prefect.outcomes[BQ_TO_ORACLE_DEPLOYMENT] = child("bq-run", "FAILED", "Failed")

    with pytest.raises(ChainStoppedError, match="BigQuery → Postgres não foi disparado"):
        flow_module.rj_smfp__nota_carioca_carga_completa(incluir_postgres=True, discord_notifications=False)

    assert BQ_TO_POSTGRES_DEPLOYMENT not in [launched[0] for launched in fake_prefect.launched]
