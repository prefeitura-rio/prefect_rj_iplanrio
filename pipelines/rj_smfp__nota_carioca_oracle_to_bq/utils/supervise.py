"""Acompanhamento dos filhos pelo pai: polling, log por filho, cancelamento dos irmãos e falha do pai."""

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.parallel import (
    FAILED_STATES,
    ParallelRunError,
    RunInfo,
    Verdict,
    all_terminal,
    decide,
    format_child_status,
    runs_to_cancel,
)


@dataclass(frozen=True)
class Supervision:
    """Como o pai observa e controla os filhos; as funções de I/O vêm de fora para o laço ser testável.

    :param read: Lê o estado atual dos flow runs pelos ids.
    :param cancel: Pede o cancelamento dos flow runs pelos ids.
    :param report: Publica uma linha de log.
    :param poll_seconds: Intervalo entre leituras.
    :param cancel_grace_seconds: Tempo máximo esperando os filhos cancelados terminarem.
    :param sleep: Função de espera (substituível nos testes).
    """

    read: Callable[[Sequence[str]], list[RunInfo]]
    cancel: Callable[[Sequence[str]], None]
    report: Callable[[str], None]
    poll_seconds: float = 30.0
    cancel_grace_seconds: float = 300.0
    sleep: Callable[[float], None] = time.sleep


def _read_and_report(children: Mapping[str, str], supervision: Supervision) -> list[RunInfo]:
    by_id = {run.run_id: run for run in supervision.read(list(children.values()))}
    runs = [by_id[run_id] for run_id in children.values()]
    for table_id, run in zip(children, runs, strict=True):
        supervision.report(format_child_status(table_id, run))
    return runs


def _poll(children: Mapping[str, str], supervision: Supervision) -> tuple[Verdict, list[RunInfo]]:
    while True:
        runs = _read_and_report(children, supervision)
        verdict = decide(runs)
        if verdict is not Verdict.RUNNING:
            return verdict, runs
        supervision.sleep(supervision.poll_seconds)


def _stop_siblings(children: Mapping[str, str], supervision: Supervision) -> None:
    """Cancela os filhos ainda ativos e espera (até o limite) que todos terminem, para a limpeza não correr com eles."""
    runs = supervision.read(list(children.values()))
    pending = runs_to_cancel(runs)
    if pending:
        supervision.report(f"Cancelando filhos ainda ativos: {[run.name for run in pending]}")
        supervision.cancel([run.run_id for run in pending])
    waited = 0.0
    while not all_terminal(runs) and waited < supervision.cancel_grace_seconds:
        supervision.sleep(min(supervision.poll_seconds, 5.0))
        waited += min(supervision.poll_seconds, 5.0)
        runs = supervision.read(list(children.values()))
    if not all_terminal(runs):
        supervision.report("Alguns filhos ainda não terminaram após o prazo de cancelamento; seguindo com a limpeza.")


def supervise(children: Mapping[str, str], supervision: Supervision) -> None:
    """Espera todos os filhos concluírem; se algum falhar, cancela os irmãos e falha sem publicar.

    Se o próprio pai for interrompido (cancelamento, SIGTERM), os filhos ativos também são cancelados.

    :param children: Flow run id de cada filho, por tabela.
    :param supervision: Funções de leitura, cancelamento e log, e os intervalos.
    :raises ParallelRunError: Se algum filho terminar sem sucesso.
    """
    try:
        verdict, runs = _poll(children, supervision)
    except BaseException:
        _stop_siblings(children, supervision)
        raise
    if verdict is Verdict.ALL_COMPLETED:
        return
    _stop_siblings(children, supervision)
    broken = [
        f"{table_id}: '{run.name}' ({run.run_id}) em {run.state_name}"
        for table_id, run in zip(children, runs, strict=True)
        if run.state_type in FAILED_STATES
    ]
    raise ParallelRunError(
        f"Filhos que falharam: {broken}. Irmãos ativos foram cancelados; as tabelas finais não foram alteradas."
    )
