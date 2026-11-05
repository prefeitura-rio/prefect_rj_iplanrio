"""Regras do passo opcional de dbt: parâmetros, espera pelo run filho e espera do BigQuery."""

import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, Protocol

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.constants import default_dbt_parameters

COMPLETED = "COMPLETED"
FINAL_STATE_TYPES = frozenset({COMPLETED, "FAILED", "CRASHED", "CANCELLED"})


class DbtRunError(RuntimeError):
    """O run de dbt não terminou com sucesso."""


@dataclass(frozen=True)
class RunStatus:
    """Estado de um flow run do Prefect.

    :param state_type: Tipo do estado (``COMPLETED``, ``FAILED``, ``RUNNING``...).
    :param state_name: Nome do estado, como aparece na UI.
    :param message: Mensagem do estado, se houver.
    :param end_time: Fim do run, em UTC, quando já terminou.
    """

    state_type: str
    state_name: str
    message: str | None
    end_time: datetime | None


class RunClient(Protocol):
    """Operações sobre o run filho de dbt."""

    def read_status(self, run_id: str) -> RunStatus:
        """Lê o estado atual do run."""
        ...

    def cancel(self, run_id: str) -> None:
        """Pede o cancelamento do run."""
        ...


def should_run_dbt(run_dbt: bool, mode: Literal["full", "synonyms_only"]) -> bool:
    """Decide se o dbt roda: só se pedido e fora do modo que apenas atualiza sinônimos.

    :param run_dbt: Parâmetro ``run_dbt`` do flow.
    :param mode: Modo do flow.
    :returns: ``True`` se o dbt deve rodar antes da carga.
    """
    return run_dbt and mode == "full"


def resolve_dbt_parameters(parameters: dict[str, object] | None) -> dict[str, object]:
    """Escolhe os parâmetros do run de dbt.

    :param parameters: Parâmetros informados; ``None`` usa os padrões da pipeline.
    :returns: Parâmetros informados, sem mesclar com os padrões, ou os padrões.
    """
    return default_dbt_parameters() if parameters is None else parameters


def skip_initial_quiet_wait(last_modified: list[datetime], dbt_finished_at: datetime | None) -> bool:
    """Decide se a espera inicial sem alterações no BigQuery pode ser dispensada.

    Só pode se o dbt rodou aqui e nenhuma tabela foi alterada depois do fim dele:
    as alterações vistas são as do próprio dbt, que já terminou.

    :param last_modified: Última alteração de cada tabela.
    :param dbt_finished_at: Fim do run de dbt, em UTC; ``None`` se o dbt não rodou.
    :returns: ``True`` se todas as tabelas foram alteradas até o fim do dbt.
    """
    return dbt_finished_at is not None and all(modified <= dbt_finished_at for modified in last_modified)


@dataclass(frozen=True)
class RunWaiter:
    """Espera o run filho de dbt terminar, consultando o estado de tempos em tempos.

    :param runs: Acesso ao estado e ao cancelamento do run.
    :param timeout_seconds: Tempo máximo de espera; passado dele o run é cancelado.
    :param poll_seconds: Intervalo entre as consultas.
    :param report: Recebe as mensagens de andamento.
    :param sleep: Função de espera, em segundos.
    :param monotonic: Relógio monotônico, em segundos.
    """

    runs: RunClient
    timeout_seconds: float
    poll_seconds: float
    report: Callable[[str], None]
    sleep: Callable[[float], None] = time.sleep
    monotonic: Callable[[], float] = time.monotonic

    def wait(self, run_id: str) -> datetime:
        """Espera o run terminar com sucesso.

        Se a espera for interrompida (cancelamento do flow pai, sinal de término) o run
        filho é cancelado antes de a interrupção seguir.

        :param run_id: Id do flow run de dbt.
        :returns: Fim do run, em UTC.
        :raises DbtRunError: Se o run terminar sem sucesso ou estourar o tempo limite.
        """
        started = self.monotonic()
        try:
            status = self._poll(run_id, started)
        except BaseException:
            self._cancel(run_id, "espera interrompida; cancelando o run de dbt")
            raise
        elapsed = self.monotonic() - started
        if status is None:
            self._cancel(
                run_id, f"tempo limite de {self.timeout_seconds / 60:.0f} min excedido; cancelando o run de dbt"
            )
            raise DbtRunError(f"O dbt não terminou em {self.timeout_seconds / 60:.0f} min; o run foi cancelado.")
        if status.state_type != COMPLETED:
            detail = f": {status.message}" if status.message else ""
            raise DbtRunError(f"O dbt terminou em {status.state_name} após {elapsed / 60:.1f} min{detail}")
        return status.end_time or datetime.now(UTC)

    def _poll(self, run_id: str, started: float) -> RunStatus | None:
        while True:
            status = self.runs.read_status(run_id)
            elapsed = self.monotonic() - started
            self.report(f"dbt {status.state_name} há {elapsed / 60:.1f} min")
            if status.state_type in FINAL_STATE_TYPES:
                return status
            if elapsed >= self.timeout_seconds:
                return None
            self.sleep(min(self.poll_seconds, self.timeout_seconds - elapsed))

    def _cancel(self, run_id: str, message: str) -> None:
        self.report(message)
        self.runs.cancel(run_id)
