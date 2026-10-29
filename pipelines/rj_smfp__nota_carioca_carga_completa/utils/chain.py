"""A cadeia da carga completa: cada deployment só roda se o anterior concluir (extração XML opcional na frente).

Não importa o Prefect: ``tasks.py`` injeta as ações (disparar, pausar, conferir, avisar) e os testes injetam fakes. O
fluxo é só uma casca em volta de :func:`run_chain`.
"""

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass

from pipelines.rj_smfp__nota_carioca_carga_completa.utils.discord_format import format_duration
from pipelines.rj_smfp__nota_carioca_carga_completa.utils.guard import ActiveRunsError

COMPLETED_STATE = "COMPLETED"
FINISHED_STATES = frozenset({"COMPLETED", "FAILED", "CRASHED", "CANCELLED"})
DISPATCH_TAG = "disparo:carga-completa"
INHERITED_TAG_PREFIXES = ("disparo:", "discord-user:")
NOT_STARTED = "Carga completa não iniciada"


def build_run_tags(current_tags: Iterable[str]) -> list[str]:
    """Tags dos flow runs filhos: a da carga completa mais as de auditoria que o bot pôs neste run.

    :param current_tags: Tags do flow run atual (o bot põe ``disparo:discord`` e ``discord-user:<id>``).
    :returns: ``disparo:carga-completa`` e as tags que começam com ``disparo:`` ou ``discord-user:``, sem repetição.
    """
    inherited = [tag for tag in current_tags if tag.startswith(INHERITED_TAG_PREFIXES)]
    return list(dict.fromkeys([DISPATCH_TAG, *inherited]))


@dataclass(frozen=True)
class ChildRun:
    """Flow run filho como devolvido pelo ``run_deployment``.

    :param run_id: Id do flow run.
    :param name: Nome do flow run.
    :param state_type: Tipo do estado (``COMPLETED``, ``FAILED``, ``RUNNING``...).
    :param state_name: Nome do estado como aparece na UI.
    """

    run_id: str
    name: str
    state_type: str
    state_name: str


@dataclass(frozen=True)
class Step:
    """Um deployment da cadeia.

    :param label: Nome nas mensagens (``Oracle → BigQuery``).
    :param deployment: ``<flow>/<deployment>``.
    :param timeout_seconds: Quanto esperar o término antes de desistir de esperar (não cancela o filho).
    :param done: Particípio que concorda com o nome (``concluído``; ``concluída`` para ``Extração XML``).
    """

    label: str
    deployment: str
    timeout_seconds: int
    done: str = "concluído"

    def __post_init__(self) -> None:
        if self.timeout_seconds <= 0:
            raise ValueError(f"O tempo limite de {self.label} precisa ser positivo, veio {self.timeout_seconds} s.")


@dataclass(frozen=True)
class ChainPlan:
    """O que encadear.

    :param first: Primeira carga (Oracle → BigQuery).
    :param second: Roda só se a primeira concluir (BigQuery → Oracle).
    :param pause_seconds: Espera entre um passo e o seguinte.
    :param tags: Tags dos flow runs filhos.
    :param extraction: Extração XML, que roda antes de tudo; ``None`` encadeia só as duas cargas.
    """

    first: Step
    second: Step
    pause_seconds: int
    tags: tuple[str, ...]
    extraction: Step | None = None

    def __post_init__(self) -> None:
        if self.pause_seconds < 0:
            raise ValueError(f"A pausa não pode ser negativa, veio {self.pause_seconds} s.")

    @property
    def steps(self) -> tuple[Step, ...]:
        """Os passos na ordem: a extração (se houver), a primeira e a segunda carga."""
        head = (self.extraction,) if self.extraction is not None else ()
        return (*head, self.first, self.second)


@dataclass(frozen=True)
class ChainActions:
    """O que a cadeia faz no mundo; cada ação é injetada.

    :param run: Dispara o deployment e espera o término ou o tempo limite; devolve o filho no último estado lido.
    :param ensure_idle: Levanta :class:`ActiveRunsError` se o deployment tiver execução ativa.
    :param pause: Espera os segundos dados.
    :param notify: Posta uma linha avulsa no Discord; nunca levanta exceção.
    :param alert: Posta a parada no Discord mencionando os cargos de alerta; nunca levanta exceção.
    :param log: Escreve no log do Prefect.
    :param link: Link de um flow run na UI, ou ``None``.
    :param clock: Relógio monotônico em segundos.
    """

    run: Callable[[Step, Sequence[str]], ChildRun]
    ensure_idle: Callable[[str], None]
    pause: Callable[[int], None]
    notify: Callable[[str], None]
    alert: Callable[[str], None]
    log: Callable[[str], None]
    link: Callable[[str], str | None]
    clock: Callable[[], float]


class ChainStoppedError(RuntimeError):
    """A cadeia parou antes de terminar: trava ocupada, ou um filho que não concluiu.

    :param message: Texto do erro do orquestrador, com nome, estado e link do filho.
    :param announcement: Linha para o Discord.
    """

    def __init__(self, message: str, announcement: str) -> None:
        super().__init__(message)
        self.announcement = announcement


class _Chain:
    """Executa os passos da cadeia sobre um plano e as ações injetadas."""

    def __init__(self, plan: ChainPlan, actions: ChainActions) -> None:
        self._plan = plan
        self._actions = actions

    def run(self) -> None:
        """Trava de todos, depois cada passo: pausa e trava antes de cada um a partir do segundo; avisa o Discord."""
        plan, actions = self._plan, self._actions
        steps = plan.steps
        started = actions.clock()
        for step in steps:
            self._ensure_idle(step, NOT_STARTED)
        sequence = ", depois ".join(step.label for step in steps)
        actions.log(f"Carga completa: {sequence}.")
        actions.notify(f"🔗 Carga completa iniciada: {sequence}")
        for index, step in enumerate(steps):
            if index > 0:
                previous = steps[index - 1]
                actions.log(f"{previous.label} {previous.done}; {step.label} começa em {plan.pause_seconds} s.")
                actions.notify(f"⏭️ {previous.label} {previous.done}; {step.label} começa em {plan.pause_seconds} s")
                actions.pause(plan.pause_seconds)
                self._ensure_idle(step, f"Carga completa interrompida ({previous.label} {previous.done})")
            self._run_step(step, skipped=steps[index + 1 :])
        took = format_duration(actions.clock() - started)
        actions.log(f"Carga completa concluída em {took}.")
        actions.notify(f"✅ Carga completa concluída em {took}")

    def _ensure_idle(self, step: Step, headline: str) -> None:
        """Confere que o deployment está livre; senão para a cadeia com o ``headline`` na mensagem."""
        try:
            self._actions.ensure_idle(step.deployment)
        except ActiveRunsError as error:
            who = ", ".join(f"{run.name} ({run.state_name})" for run in error.runs)
            raise ChainStoppedError(
                f"{headline}: {step.label} ({step.deployment}) tem {error}",
                f"⛔ {headline}: {step.label} já tem execução ativa ({who})",
            ) from error

    def _run_step(self, step: Step, skipped: Sequence[Step]) -> None:
        """Dispara o deployment, espera, registra o filho com o link e para a cadeia se ele não concluiu.

        :param step: Deployment a rodar.
        :param skipped: Os passos que deixam de ser disparados se este não concluir; vazio no último.
        """
        actions = self._actions
        actions.log(
            f"Disparando {step.label} ({step.deployment}); espera de até {format_duration(step.timeout_seconds)}."
        )
        child = actions.run(step, self._plan.tags)
        url = actions.link(child.run_id)
        actions.log(f"{step.label}: flow run {child.name} em {child.state_name}{' ' + url if url else ''}")
        if child.state_type != COMPLETED_STATE:
            raise self._unfinished(step, child, skipped)

    def _unfinished(self, step: Step, child: ChildRun, skipped: Sequence[Step]) -> ChainStoppedError:
        """Monta a parada por um filho que terminou mal ou que ainda corria quando o tempo limite acabou."""
        url = self._actions.link(child.run_id)
        if child.state_type in FINISHED_STATES:
            outcome = f"terminou em {child.state_name}"
        else:
            limit = format_duration(step.timeout_seconds)
            outcome = (
                f"ainda estava em {child.state_name} após o limite de {limit}; "
                "o flow run segue ativo e não foi cancelado"
            )
        tail = _not_triggered(skipped)
        detail = f"flow run {child.name}, id {child.run_id}{', ' + url if url else ''}"
        return ChainStoppedError(
            f"Carga completa interrompida: {step.label} {outcome} ({detail}){tail}.",
            f"⛔ Carga completa interrompida: {step.label} {outcome}{tail}{' · <' + url + '>' if url else ''}",
        )


def _not_triggered(skipped: Sequence[Step]) -> str:
    """``; X não foi disparado`` ou ``; X e Y não foram disparados``; vazio se nada deixou de rodar."""
    labels = [step.label for step in skipped]
    if not labels:
        return ""
    if len(labels) == 1:
        return f"; {labels[0]} não foi disparado"
    return f"; {', '.join(labels[:-1])} e {labels[-1]} não foram disparados"


def run_chain(plan: ChainPlan, actions: ChainActions) -> None:
    """Roda a cadeia, avisa o Discord em cada passo e falha o orquestrador se qualquer parte não concluir.

    As paradas saem como alerta (com os cargos mencionados); início, transições e conclusão, como aviso simples.

    :param plan: Os deployments, a pausa e as tags.
    :param actions: Ações do mundo (disparar, pausar, conferir, avisar).
    :raises ChainStoppedError: Se a trava achar execução ativa, ou um filho não terminar em ``Completed``.
    """
    try:
        _Chain(plan, actions).run()
    except ChainStoppedError as stopped:
        actions.alert(stopped.announcement)
        raise
    except Exception as error:  # qualquer falha inesperada também alerta o Discord antes de falhar o orquestrador
        actions.alert(f"⛔ Carga completa interrompida por erro: {type(error).__name__}: {error}")
        raise
