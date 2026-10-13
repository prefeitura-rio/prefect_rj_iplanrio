"""A cadeia da carga completa: o primeiro deployment e, só se ele concluir, o segundo.

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
    """

    label: str
    deployment: str
    timeout_seconds: int

    def __post_init__(self) -> None:
        if self.timeout_seconds <= 0:
            raise ValueError(f"O tempo limite de {self.label} precisa ser positivo, veio {self.timeout_seconds} s.")


@dataclass(frozen=True)
class ChainPlan:
    """O que encadear.

    :param first: Roda primeiro.
    :param second: Roda só se o primeiro concluir.
    :param pause_seconds: Espera entre os dois.
    :param tags: Tags dos dois flow runs filhos.
    """

    first: Step
    second: Step
    pause_seconds: int
    tags: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.pause_seconds < 0:
            raise ValueError(f"A pausa não pode ser negativa, veio {self.pause_seconds} s.")


@dataclass(frozen=True)
class ChainActions:
    """O que a cadeia faz no mundo; cada ação é injetada.

    :param run: Dispara o deployment e espera o término ou o tempo limite; devolve o filho no último estado lido.
    :param ensure_idle: Levanta :class:`ActiveRunsError` se o deployment tiver execução ativa.
    :param pause: Espera os segundos dados.
    :param notify: Posta uma linha avulsa no Discord; nunca levanta exceção.
    :param log: Escreve no log do Prefect.
    :param link: Link de um flow run na UI, ou ``None``.
    :param clock: Relógio monotônico em segundos.
    """

    run: Callable[[Step, Sequence[str]], ChildRun]
    ensure_idle: Callable[[str], None]
    pause: Callable[[int], None]
    notify: Callable[[str], None]
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
        """Trava, primeiro, pausa, trava do segundo, segundo; avisa o Discord a cada passo."""
        plan, actions = self._plan, self._actions
        started = actions.clock()
        self._ensure_idle(plan.first, NOT_STARTED)
        self._ensure_idle(plan.second, NOT_STARTED)
        actions.log(f"Carga completa: {plan.first.label}, depois {plan.second.label}.")
        actions.notify(f"🔗 Carga completa iniciada: {plan.first.label}, depois {plan.second.label}")
        self._run_step(plan.first, skipped=plan.second)
        actions.log(f"{plan.first.label} concluído; {plan.second.label} começa em {plan.pause_seconds} s.")
        actions.notify(f"⏭️ {plan.first.label} concluído; {plan.second.label} começa em {plan.pause_seconds} s")
        actions.pause(plan.pause_seconds)
        self._ensure_idle(plan.second, f"Carga completa interrompida ({plan.first.label} concluído)")
        self._run_step(plan.second, skipped=None)
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

    def _run_step(self, step: Step, skipped: Step | None) -> None:
        """Dispara o deployment, espera, registra o filho com o link e para a cadeia se ele não concluiu.

        :param step: Deployment a rodar.
        :param skipped: O passo que deixa de ser disparado se este não concluir; ``None`` no último.
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

    def _unfinished(self, step: Step, child: ChildRun, skipped: Step | None) -> ChainStoppedError:
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
        tail = f"; {skipped.label} não foi disparado" if skipped else ""
        detail = f"flow run {child.name}, id {child.run_id}{', ' + url if url else ''}"
        return ChainStoppedError(
            f"Carga completa interrompida: {step.label} {outcome} ({detail}){tail}.",
            f"⛔ Carga completa interrompida: {step.label} {outcome}{tail}{' · <' + url + '>' if url else ''}",
        )


def run_chain(plan: ChainPlan, actions: ChainActions) -> None:
    """Roda a cadeia, avisa o Discord em cada passo e falha o orquestrador se qualquer parte não concluir.

    :param plan: Os dois deployments, a pausa e as tags.
    :param actions: Ações do mundo (disparar, pausar, conferir, avisar).
    :raises ChainStoppedError: Se a trava achar execução ativa, ou um filho não terminar em ``Completed``.
    """
    try:
        _Chain(plan, actions).run()
    except ChainStoppedError as stopped:
        actions.notify(stopped.announcement)
        raise
    except Exception as error:  # qualquer falha inesperada também avisa o Discord antes de falhar o orquestrador
        actions.notify(f"⛔ Carga completa interrompida por erro: {type(error).__name__}: {error}")
        raise
