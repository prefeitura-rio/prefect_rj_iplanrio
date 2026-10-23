from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

import pytest

from pipelines.rj_smfp__nota_carioca_carga_completa.constants import (
    BQ_TO_ORACLE_DEPLOYMENT,
    BQ_TO_ORACLE_LABEL,
    BQ_TO_POSTGRES_DEPLOYMENT,
    BQ_TO_POSTGRES_LABEL,
    ORACLE_TO_BQ_DEPLOYMENT,
    ORACLE_TO_BQ_LABEL,
)
from pipelines.rj_smfp__nota_carioca_carga_completa.utils.chain import (
    ChainActions,
    ChainPlan,
    ChainStoppedError,
    ChildRun,
    ExportManifest,
    PostgresLoad,
    Step,
    build_run_tags,
    run_chain,
)
from pipelines.rj_smfp__nota_carioca_carga_completa.utils.guard import ActiveRun, ActiveRunsError

ORACLE_STEP = Step(ORACLE_TO_BQ_LABEL, ORACLE_TO_BQ_DEPLOYMENT, 4 * 3600)
BQ_STEP = Step(BQ_TO_ORACLE_LABEL, BQ_TO_ORACLE_DEPLOYMENT, 8 * 3600)
TAGS = ("disparo:carga-completa", "disparo:discord", "discord-user:42")
PLAN = ChainPlan(first=ORACLE_STEP, second=BQ_STEP, pause_seconds=30, tags=TAGS)
ORACLE_DONE_SECONDS = 40 * 60
BQ_DONE_SECONDS = 3 * 3600
POSTGRES_STEP = Step(BQ_TO_POSTGRES_LABEL, BQ_TO_POSTGRES_DEPLOYMENT, 4 * 3600)
EXPORT = ExportManifest(bucket="bucket-do-export", dataset_id="nota_carioca")
POSTGRES_PLAN = ChainPlan(
    first=ORACLE_STEP, second=BQ_STEP, pause_seconds=30, tags=TAGS, postgres=PostgresLoad(POSTGRES_STEP, EXPORT)
)


def child(name: str, state_type: str, state_name: str) -> ChildRun:
    return ChildRun(run_id=f"id-{name}", name=name, state_type=state_type, state_name=state_name)


COMPLETED = ("COMPLETED", "Completed")


@dataclass
class World:
    """Mundo falso: devolve o resultado combinado de cada deployment e registra o que a cadeia fez, em ordem."""

    results: dict[str, ChildRun | Exception]
    busy: dict[str, list[ActiveRun]] = field(default_factory=dict)
    busy_after_pause: dict[str, list[ActiveRun]] = field(default_factory=dict)
    ui_url: str | None = "https://prefect.test"
    events: list[tuple[str, ...]] = field(default_factory=list)
    run_tags: list[Sequence[str]] = field(default_factory=list)
    run_parameters: list[Mapping[str, object]] = field(default_factory=list)
    now: float = 0.0
    paused: bool = False

    def run(self, step: Step, tags: Sequence[str]) -> ChildRun:
        self.events.append(("run", step.deployment, str(step.timeout_seconds)))
        self.run_tags.append(tags)
        self.run_parameters.append(step.parameters)
        self.now += ORACLE_DONE_SECONDS if step is ORACLE_STEP else BQ_DONE_SECONDS
        outcome = self.results[step.deployment]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    def ensure_idle(self, deployment: str) -> None:
        self.events.append(("guard", deployment))
        blocking = (self.busy_after_pause if self.paused else self.busy).get(deployment)
        if blocking:
            raise ActiveRunsError("ocupado", blocking)

    def pause(self, seconds: int) -> None:
        self.events.append(("pause", str(seconds)))
        self.paused = True
        self.now += seconds

    def notify(self, text: str) -> None:
        self.events.append(("notify", text))

    def log(self, text: str) -> None:
        self.events.append(("log", text))

    def link(self, run_id: str) -> str | None:
        return f"{self.ui_url}/runs/flow-run/{run_id}" if self.ui_url else None

    def actions(self) -> ChainActions:
        return ChainActions(
            run=self.run,
            ensure_idle=self.ensure_idle,
            pause=self.pause,
            notify=self.notify,
            log=self.log,
            link=self.link,
            clock=lambda: self.now,
        )

    def of_kind(self, kind: str) -> list[tuple[str, ...]]:
        return [event for event in self.events if event[0] == kind]

    def notifications(self) -> list[str]:
        return [event[1] for event in self.of_kind("notify")]

    def runs(self) -> list[str]:
        return [event[1] for event in self.of_kind("run")]


def both(oracle: ChildRun | Exception, bq: ChildRun | Exception | None = None) -> dict[str, ChildRun | Exception]:
    return {
        ORACLE_TO_BQ_DEPLOYMENT: oracle,
        BQ_TO_ORACLE_DEPLOYMENT: bq if bq is not None else child("bq-run", *COMPLETED),
    }


def with_postgres(results: dict[str, ChildRun | Exception], postgres: ChildRun | Exception | None = None) -> World:
    return World({**results, BQ_TO_POSTGRES_DEPLOYMENT: postgres or child("pg-run", *COMPLETED)})


def idle_world() -> World:
    return World(both(child("oracle-run", *COMPLETED)))


def busy_run(name: str = "manual-run") -> ActiveRun:
    return ActiveRun(
        run_id=f"id-{name}", name=name, state_type="RUNNING", state_name="Running", expected_start_time=None
    )


def test_run_chain_runs_second_after_pause_when_first_completes() -> None:
    # Given: os dois deployments livres e o primeiro conclui
    world = idle_world()

    # When: roda a cadeia
    run_chain(PLAN, world.actions())

    # Then: confere os dois, dispara o primeiro, pausa 30 s, reconfere só o segundo e dispara o segundo
    assert [event for event in world.events if event[0] != "log" and event[0] != "notify"] == [
        ("guard", ORACLE_TO_BQ_DEPLOYMENT),
        ("guard", BQ_TO_ORACLE_DEPLOYMENT),
        ("run", ORACLE_TO_BQ_DEPLOYMENT, str(4 * 3600)),
        ("pause", "30"),
        ("guard", BQ_TO_ORACLE_DEPLOYMENT),
        ("run", BQ_TO_ORACLE_DEPLOYMENT, str(8 * 3600)),
    ]


def test_run_chain_announces_start_transition_and_total_duration() -> None:
    world = idle_world()

    run_chain(PLAN, world.actions())

    assert world.notifications() == [
        "🔗 Carga completa iniciada: Oracle → BigQuery, depois BigQuery → Oracle",
        "⏭️ Oracle → BigQuery concluído; BigQuery → Oracle começa em 30 s",
        "✅ Carga completa concluída em 3h 40min",
    ]


def test_run_chain_passes_the_same_tags_to_both_children() -> None:
    world = idle_world()

    run_chain(PLAN, world.actions())

    assert world.run_tags == [TAGS, TAGS]


def test_run_chain_logs_child_runs_with_ui_links() -> None:
    world = idle_world()

    run_chain(PLAN, world.actions())

    logs = [event[1] for event in world.of_kind("log")]
    assert (
        "Oracle → BigQuery: flow run oracle-run em Completed https://prefect.test/runs/flow-run/id-oracle-run" in logs
    )
    assert "BigQuery → Oracle: flow run bq-run em Completed https://prefect.test/runs/flow-run/id-bq-run" in logs


@pytest.mark.parametrize(
    ("state_type", "state_name"),
    [("FAILED", "Failed"), ("CRASHED", "Crashed"), ("CANCELLED", "Cancelled")],
)
def test_run_chain_stops_without_second_when_first_does_not_complete(state_type: str, state_name: str) -> None:
    # Given: o primeiro termina mal
    world = World(both(child("oracle-run", state_type, state_name)))

    # When: roda a cadeia
    with pytest.raises(ChainStoppedError) as stopped:
        run_chain(PLAN, world.actions())

    # Then: o segundo não é disparado nem há pausa, e o erro traz nome, estado e link do filho
    assert world.runs() == [ORACLE_TO_BQ_DEPLOYMENT]
    assert world.of_kind("pause") == []
    message = str(stopped.value)
    assert f"terminou em {state_name}" in message
    assert "oracle-run" in message
    assert "https://prefect.test/runs/flow-run/id-oracle-run" in message
    assert "BigQuery → Oracle não foi disparado" in message


def test_run_chain_announces_stop_with_state_and_link_when_first_fails() -> None:
    world = World(both(child("oracle-run", "FAILED", "Failed")))

    with pytest.raises(ChainStoppedError):
        run_chain(PLAN, world.actions())

    assert world.notifications()[-1] == (
        "⛔ Carga completa interrompida: Oracle → BigQuery terminou em Failed; "
        "BigQuery → Oracle não foi disparado · <https://prefect.test/runs/flow-run/id-oracle-run>"
    )
    assert not any("começa em" in text or "concluída" in text for text in world.notifications())


def test_run_chain_stops_without_cancelling_when_first_is_still_running_after_timeout() -> None:
    # Given: o tempo limite acabou e o primeiro ainda corre
    world = World(both(child("oracle-run", "RUNNING", "Running")))

    # When: roda a cadeia
    with pytest.raises(ChainStoppedError) as stopped:
        run_chain(PLAN, world.actions())

    # Then: para sem disparar o segundo e diz que o filho segue ativo, sem ser cancelado
    assert world.runs() == [ORACLE_TO_BQ_DEPLOYMENT]
    assert "ainda estava em Running após o limite de 4h 00min" in str(stopped.value)
    assert "não foi cancelado" in str(stopped.value)
    assert "ainda estava em Running" in world.notifications()[-1]
    assert "BigQuery → Oracle não foi disparado" in world.notifications()[-1]


def test_run_chain_fails_when_second_does_not_complete() -> None:
    # Given: o primeiro conclui e o segundo falha
    world = World(both(child("oracle-run", *COMPLETED), child("bq-run", "FAILED", "Failed")))

    # When: roda a cadeia
    with pytest.raises(ChainStoppedError) as stopped:
        run_chain(PLAN, world.actions())

    # Then: o segundo foi disparado depois da pausa e o orquestrador falha com o estado dele
    assert world.runs() == [ORACLE_TO_BQ_DEPLOYMENT, BQ_TO_ORACLE_DEPLOYMENT]
    assert "BigQuery → Oracle terminou em Failed" in str(stopped.value)
    assert "bq-run" in str(stopped.value)
    assert world.notifications()[-1] == (
        "⛔ Carga completa interrompida: BigQuery → Oracle terminou em Failed"
        " · <https://prefect.test/runs/flow-run/id-bq-run>"
    )


def test_run_chain_fails_when_second_is_still_running_after_timeout() -> None:
    world = World(both(child("oracle-run", *COMPLETED), child("bq-run", "RUNNING", "Running")))

    with pytest.raises(ChainStoppedError) as stopped:
        run_chain(PLAN, world.actions())

    assert "BigQuery → Oracle ainda estava em Running após o limite de 8h 00min" in str(stopped.value)
    assert not any("concluída" in text for text in world.notifications())


@pytest.mark.parametrize("busy_deployment", [ORACLE_TO_BQ_DEPLOYMENT, BQ_TO_ORACLE_DEPLOYMENT])
def test_run_chain_triggers_nothing_when_either_deployment_is_busy_at_start(busy_deployment: str) -> None:
    # Given: alguém disparou um dos dois deployments à mão
    world = idle_world()
    world.busy[busy_deployment] = [busy_run()]

    # When: tenta iniciar a cadeia
    with pytest.raises(ChainStoppedError) as stopped:
        run_chain(PLAN, world.actions())

    # Then: nada é disparado, não há aviso de início e o erro nomeia a execução em andamento
    assert world.of_kind("run") == []
    assert world.of_kind("pause") == []
    assert world.notifications() == [
        "⛔ Carga completa não iniciada: " + _label(busy_deployment) + " já tem execução ativa (manual-run (Running))"
    ]
    assert "Carga completa não iniciada" in str(stopped.value)
    assert busy_deployment in str(stopped.value)


def _label(deployment: str) -> str:
    return ORACLE_TO_BQ_LABEL if deployment == ORACLE_TO_BQ_DEPLOYMENT else BQ_TO_ORACLE_LABEL


def test_run_chain_does_not_run_second_when_it_became_busy_during_the_pause() -> None:
    # Given: o segundo estava livre no início, mas alguém o disparou durante a pausa
    world = idle_world()
    world.busy_after_pause[BQ_TO_ORACLE_DEPLOYMENT] = [busy_run()]

    # When: roda a cadeia
    with pytest.raises(ChainStoppedError) as stopped:
        run_chain(PLAN, world.actions())

    # Then: o primeiro rodou, o segundo não, e a parada avisa o Discord
    assert world.runs() == [ORACLE_TO_BQ_DEPLOYMENT]
    assert "BigQuery → Oracle" in str(stopped.value)
    assert world.notifications()[-1] == (
        "⛔ Carga completa interrompida (Oracle → BigQuery concluído): "
        "BigQuery → Oracle já tem execução ativa (manual-run (Running))"
    )


def test_run_chain_rechecks_only_the_second_deployment_after_the_pause() -> None:
    world = idle_world()

    run_chain(PLAN, world.actions())

    guards = [event[1] for event in world.of_kind("guard")]
    assert guards == [ORACLE_TO_BQ_DEPLOYMENT, BQ_TO_ORACLE_DEPLOYMENT, BQ_TO_ORACLE_DEPLOYMENT]


def test_run_chain_announces_and_reraises_unexpected_errors() -> None:
    # Given: o disparo do primeiro estoura por um erro de infraestrutura
    world = World(both(RuntimeError("API fora do ar")))

    # When: roda a cadeia
    with pytest.raises(RuntimeError, match="API fora do ar"):
        run_chain(PLAN, world.actions())

    # Then: o Discord é avisado e o segundo não é disparado
    assert world.notifications()[-1] == "⛔ Carga completa interrompida por erro: RuntimeError: API fora do ar"
    assert world.runs() == [ORACLE_TO_BQ_DEPLOYMENT]


def test_run_chain_omits_links_when_ui_url_is_unknown() -> None:
    world = World(both(child("oracle-run", "FAILED", "Failed")), ui_url=None)

    with pytest.raises(ChainStoppedError) as stopped:
        run_chain(PLAN, world.actions())

    assert world.notifications()[-1] == (
        "⛔ Carga completa interrompida: Oracle → BigQuery terminou em Failed; BigQuery → Oracle não foi disparado"
    )
    assert "https://" not in str(stopped.value)


def test_build_run_tags_always_tags_the_chain() -> None:
    assert build_run_tags([]) == ["disparo:carga-completa"]


def test_build_run_tags_propagates_audit_tags_of_the_orchestrator_run() -> None:
    # Given: o bot tagueou o run do orquestrador, e o deployment traz tags próprias
    current = [
        "environment:prod",
        "disparo:discord",
        "severity:medium",
        "discord-user:123456789",
        "code_owner:fredzolio",
    ]

    # When: monta as tags dos filhos
    tags = build_run_tags(current)

    # Then: herda só as de auditoria, depois da tag da carga completa
    assert tags == ["disparo:carga-completa", "disparo:discord", "discord-user:123456789"]


def test_build_run_tags_does_not_repeat_the_chain_tag() -> None:
    assert build_run_tags(["disparo:carga-completa", "disparo:discord"]) == [
        "disparo:carga-completa",
        "disparo:discord",
    ]


def test_step_rejects_non_positive_timeout() -> None:
    with pytest.raises(ValueError, match="tempo limite"):
        Step(ORACLE_TO_BQ_LABEL, ORACLE_TO_BQ_DEPLOYMENT, 0)


def test_chain_plan_rejects_negative_pause() -> None:
    with pytest.raises(ValueError, match="pausa"):
        ChainPlan(first=ORACLE_STEP, second=BQ_STEP, pause_seconds=-1, tags=())


def test_the_export_manifest_uri_is_derived_from_the_flow_run_of_bq_to_oracle():
    assert EXPORT.uri("abc-123") == "gs://bucket-do-export/nota_carioca/abc-123/manifesto.json"


def test_run_chain_loads_postgres_after_bq_to_oracle_with_the_manifest_of_its_run() -> None:
    # Given: os três deployments livres e os dois primeiros concluem
    world = with_postgres(both(child("oracle-run", *COMPLETED)))

    # When: roda a cadeia com o passo do Postgres
    run_chain(POSTGRES_PLAN, world.actions())

    # Then: confere os três, dispara na ordem com pausas, e o Postgres recebe o manifesto do filho bq_to_oracle
    assert [event for event in world.events if event[0] not in ("log", "notify")] == [
        ("guard", ORACLE_TO_BQ_DEPLOYMENT),
        ("guard", BQ_TO_ORACLE_DEPLOYMENT),
        ("guard", BQ_TO_POSTGRES_DEPLOYMENT),
        ("run", ORACLE_TO_BQ_DEPLOYMENT, str(4 * 3600)),
        ("pause", "30"),
        ("guard", BQ_TO_ORACLE_DEPLOYMENT),
        ("run", BQ_TO_ORACLE_DEPLOYMENT, str(8 * 3600)),
        ("pause", "30"),
        ("guard", BQ_TO_POSTGRES_DEPLOYMENT),
        ("run", BQ_TO_POSTGRES_DEPLOYMENT, str(4 * 3600)),
    ]
    assert world.run_parameters[2] == {"manifesto_uri": "gs://bucket-do-export/nota_carioca/id-bq-run/manifesto.json"}


def test_run_chain_announces_the_postgres_step_in_start_transition_and_total() -> None:
    world = with_postgres(both(child("oracle-run", *COMPLETED)))

    run_chain(POSTGRES_PLAN, world.actions())

    assert world.notifications() == [
        "🔗 Carga completa iniciada: Oracle → BigQuery, depois BigQuery → Oracle, depois BigQuery → Postgres",
        "⏭️ Oracle → BigQuery concluído; BigQuery → Oracle começa em 30 s",
        "⏭️ BigQuery → Oracle concluído; BigQuery → Postgres começa em 30 s",
        "✅ Carga completa concluída em 6h 41min",
    ]


@pytest.mark.parametrize(("state_type", "state_name"), [("FAILED", "Failed"), ("CRASHED", "Crashed")])
def test_run_chain_does_not_load_postgres_when_bq_to_oracle_fails(state_type: str, state_name: str) -> None:
    # Given: o BigQuery → Oracle termina mal
    world = with_postgres(both(child("oracle-run", *COMPLETED), child("bq-run", state_type, state_name)))

    # When: roda a cadeia
    with pytest.raises(ChainStoppedError) as stopped:
        run_chain(POSTGRES_PLAN, world.actions())

    # Then: o Postgres não é disparado (a foto fica alinhada com o Oracle) e a parada diz isso
    assert world.runs() == [ORACLE_TO_BQ_DEPLOYMENT, BQ_TO_ORACLE_DEPLOYMENT]
    assert f"BigQuery → Oracle terminou em {state_name}" in str(stopped.value)
    assert "BigQuery → Postgres não foi disparado" in str(stopped.value)
    assert world.notifications()[-1].startswith(
        f"⛔ Carga completa interrompida: BigQuery → Oracle terminou em {state_name}; BigQuery → Postgres não foi"
    )


def test_run_chain_fails_when_the_postgres_load_does_not_complete() -> None:
    world = with_postgres(both(child("oracle-run", *COMPLETED)), child("pg-run", "FAILED", "Failed"))

    with pytest.raises(ChainStoppedError) as stopped:
        run_chain(POSTGRES_PLAN, world.actions())

    assert world.runs()[-1] == BQ_TO_POSTGRES_DEPLOYMENT
    assert "BigQuery → Postgres terminou em Failed" in str(stopped.value)
    assert not any("concluída" in text for text in world.notifications())


def test_run_chain_triggers_nothing_when_the_postgres_deployment_is_busy_at_start() -> None:
    world = with_postgres(both(child("oracle-run", *COMPLETED)))
    world.busy[BQ_TO_POSTGRES_DEPLOYMENT] = [busy_run()]

    with pytest.raises(ChainStoppedError, match="BigQuery → Postgres"):
        run_chain(POSTGRES_PLAN, world.actions())

    assert world.of_kind("run") == []


def test_run_chain_does_not_load_postgres_when_it_became_busy_during_the_second_pause() -> None:
    world = with_postgres(both(child("oracle-run", *COMPLETED)))
    world.busy_after_pause[BQ_TO_POSTGRES_DEPLOYMENT] = [busy_run()]

    with pytest.raises(ChainStoppedError):
        run_chain(POSTGRES_PLAN, world.actions())

    assert world.runs() == [ORACLE_TO_BQ_DEPLOYMENT, BQ_TO_ORACLE_DEPLOYMENT]
    assert world.notifications()[-1] == (
        "⛔ Carga completa interrompida (BigQuery → Oracle concluído): "
        "BigQuery → Postgres já tem execução ativa (manual-run (Running))"
    )


def test_the_default_plan_has_no_postgres_step_and_no_step_parameters() -> None:
    world = idle_world()

    run_chain(PLAN, world.actions())

    assert world.run_parameters == [{}, {}]
    assert BQ_TO_POSTGRES_DEPLOYMENT not in world.runs()
