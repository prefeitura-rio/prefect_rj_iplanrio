"""Flow da paridade F1: dbt, leitura do resumo, relatório, Discord e falha em divergência."""

import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from functools import partial
from types import SimpleNamespace

import httpx
import pytest
from prefect.artifacts import Artifact
from prefect.testing.utilities import prefect_test_harness

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.dbt import DbtRunError, RunStatus
from pipelines.rj_smfp__nota_carioca_paridade_f1 import flow as flow_module
from pipelines.rj_smfp__nota_carioca_paridade_f1 import tasks as tasks_module
from pipelines.rj_smfp__nota_carioca_paridade_f1.constants import DBT_DEPLOYMENT, REPORT_ARTIFACT_KEY
from pipelines.rj_smfp__nota_carioca_paridade_f1.utils import resumo as resumo_module
from pipelines.rj_smfp__nota_carioca_paridade_f1.utils.discord import Environment
from pipelines.rj_smfp__nota_carioca_paridade_f1.utils.notify import make_notifier
from pipelines.rj_smfp__nota_carioca_paridade_f1.utils.paridade import EXPECTED_RULES, ParidadeReprovadaError

WEBHOOK = "https://discord.example/api/webhooks/1/fake-token"
CORTE_NSU = 1_500_000


@pytest.fixture(scope="module", autouse=True)
def prefect_api() -> Iterator[None]:
    """API temporária do Prefect (SQLite): as tasks e o flow rodam de verdade, sem tocar no Prefect real."""
    with prefect_test_harness():
        yield


def summary_rows(*extra: tuple[str, str, int]) -> list[dict[str, object]]:
    # Gerado "no futuro" em relação ao início do flow: o dbt falso termina na hora.
    generated = datetime.now(UTC) + timedelta(minutes=5)
    base = [(regra, "igual", 100 + index) for index, regra in enumerate(EXPECTED_RULES)]
    return [
        {"regra": regra, "classe": classe, "quantidade": quantidade, "corte_nsu": CORTE_NSU, "gerado_em": generated}
        for regra, classe, quantidade in [*base, *extra]
    ]


class FakeEnvironment:
    """Só o que sai do processo: o run de dbt no Prefect, o BigQuery e o webhook do Discord."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.rows = summary_rows()
        self.dbt_state = RunStatus("COMPLETED", "Completed", None, datetime.now(UTC))
        self.dbt_launches: list[tuple[str, dict[str, object]]] = []
        self.discord: list[dict[str, object]] = []
        self.reads: list[tuple[str, str]] = []
        env = self

        class FakeRuns:
            def read_status(self, run_id: str) -> RunStatus:
                return env.dbt_state

            def cancel(self, run_id: str) -> None:
                raise AssertionError("o run de dbt não deveria ser cancelado")

        class FakeReader:
            def __init__(self, project: str, table: str) -> None:
                env.reads.append((project, table))

            def read(self) -> list[dict[str, object]]:
                return env.rows

        def start(deployment: str, parameters: dict[str, object]) -> SimpleNamespace:
            self.dbt_launches.append((deployment, parameters))
            return SimpleNamespace(id="dbt-run", name="dbt", url=None)

        def handler(request: httpx.Request) -> httpx.Response:
            self.discord.append(json.loads(request.content))
            return httpx.Response(200, json={"id": "1"})

        monkeypatch.setattr(flow_module, "inject_bd_credentials_task", lambda **_: None)
        monkeypatch.setattr(tasks_module.runs, "start_deployment_run", start)
        monkeypatch.setattr(tasks_module.runs, "PrefectRuns", FakeRuns)
        monkeypatch.setattr(resumo_module, "BigQueryResumoReader", FakeReader)
        monkeypatch.setattr(
            flow_module,
            "make_notifier",
            partial(make_notifier, resolve=lambda: WEBHOOK, transport=httpx.MockTransport(handler)),
        )


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch) -> FakeEnvironment:
    return FakeEnvironment(monkeypatch)


def published_report() -> dict[str, object]:
    artifact = Artifact.get(REPORT_ARTIFACT_KEY)
    assert artifact is not None
    start = artifact.data.index("```json\n") + len("```json\n")
    return json.loads(artifact.data[start : artifact.data.index("\n```", start)])


def test_flow_succeeds_when_every_rule_matches(env: FakeEnvironment) -> None:
    # Given: o dbt conclui e o resumo só tem linhas iguais nas 11 regras
    # When: o flow roda
    flow_module.rj_smfp__nota_carioca_paridade_f1()

    # Then: o relatório publicado como artefato está aprovado com divergente=0 e comparados>0 por regra
    report = published_report()
    assert report["resultado"] == "aprovado"
    assert [item["regra"] for item in report["regras"]] == list(EXPECTED_RULES)
    assert all(item["divergentes"] == 0 and item["comparados"] > 0 for item in report["regras"])
    # E: o Discord recebeu uma única mensagem de sucesso, sem o marcador de staging
    assert len(env.discord) == 1
    assert env.discord[0]["content"].startswith("✅")
    assert "STAGING" not in env.discord[0]["content"]


def test_flow_runs_the_paridade_dbt_build_with_vars_through_the_shared_dbt_deployment(env: FakeEnvironment) -> None:
    # Given: um corte de NSU pedido
    env.rows = [{**item, "corte_nsu": 777} for item in env.rows]

    # When: o flow roda com o corte
    flow_module.rj_smfp__nota_carioca_paridade_f1(corte_nsu=777)

    # Then: o deployment de dbt compartilhado recebe o build de tag:paridade_nc2 com as vars da trilha dbt
    assert [deployment for deployment, _ in env.dbt_launches] == [DBT_DEPLOYMENT]
    parameters = env.dbt_launches[0][1]
    assert parameters["command"] == "build"
    assert parameters["select"] == "tag:paridade_nc2"
    assert parameters["flag"] == '--vars={"nota_carioca_v2_habilitado": true, "nc2_corte_nsu": 777}'
    # E: o resumo é lido da tabela do contrato
    assert env.reads == [("rj-iplanrio-dia", "rj-iplanrio-dia.nota_carioca_v2.paridade_nc2__resumo")]


def test_flow_omits_the_nsu_cut_var_when_none_is_given(env: FakeEnvironment) -> None:
    # Given / When: sem corte pedido (o dbt o calcula)
    flow_module.rj_smfp__nota_carioca_paridade_f1()

    # Then: só a variável de habilitação vai ao dbt
    assert env.dbt_launches[0][1]["flag"] == '--vars={"nota_carioca_v2_habilitado": true}'


def test_flow_fails_naming_the_rule_and_notifies_discord_on_divergence(env: FakeEnvironment) -> None:
    # Given: um centavo de diferença em R-11
    env.rows = summary_rows(("R-11", "divergente", 1))

    # When / Then: o flow falha citando R-11
    with pytest.raises(ParidadeReprovadaError, match="R-11"):
        flow_module.rj_smfp__nota_carioca_paridade_f1()

    # E: o relatório publicado está reprovado e o Discord recebeu o erro com a regra
    assert published_report()["resultado"] == "reprovado"
    assert len(env.discord) == 1
    assert env.discord[0]["content"].startswith("❌")
    assert "R-11" in env.discord[0]["content"]


def test_flow_fails_when_a_rule_compared_nothing(env: FakeEnvironment) -> None:
    # Given: R-02 sumiu do resumo
    env.rows = [item for item in env.rows if item["regra"] != "R-02"]

    # When / Then: falha citando R-02
    with pytest.raises(ParidadeReprovadaError, match="R-02"):
        flow_module.rj_smfp__nota_carioca_paridade_f1()
    assert "R-02" in env.discord[0]["content"]


def test_flow_fails_and_notifies_when_the_dbt_run_fails_without_reading_the_summary(env: FakeEnvironment) -> None:
    # Given: o run de dbt termina em Failed
    env.dbt_state = RunStatus("FAILED", "Failed", "teste de dados quebrou", datetime.now(UTC))

    # When / Then: o flow falha com o erro do dbt, o resumo não é lido e o Discord recebe o erro
    with pytest.raises(DbtRunError, match="Failed"):
        flow_module.rj_smfp__nota_carioca_paridade_f1()
    assert env.reads == []
    assert env.discord[0]["content"].startswith("❌")
    assert "dbt" in env.discord[0]["content"]


def test_staging_marks_discord_messages(env: FakeEnvironment, monkeypatch: pytest.MonkeyPatch) -> None:
    # Given: o deployment é o de staging
    monkeypatch.setattr(flow_module, "current_environment", lambda: Environment.STAGING)

    # When: o flow roda com sucesso
    flow_module.rj_smfp__nota_carioca_paridade_f1()

    # Then: a mensagem leva o marcador de staging
    assert env.discord[0]["content"].startswith("🧪 STAGING · ✅")


def test_discord_can_be_disabled(env: FakeEnvironment) -> None:
    # Given / When: o flow roda com as notificações desligadas
    flow_module.rj_smfp__nota_carioca_paridade_f1(discord_notifications=False)

    # Then: nenhuma requisição ao webhook
    assert env.discord == []
