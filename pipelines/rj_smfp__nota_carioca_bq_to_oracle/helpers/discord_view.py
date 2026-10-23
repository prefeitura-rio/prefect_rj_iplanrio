"""Estado e limites do embed de progresso do Discord: o que a execução mostra."""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

COLOR_RUNNING = 3447003


COLOR_SUCCESS = 3066993


COLOR_FAILURE = 15158332


COLOR_CANCELLED = 9807270


TITLE_LIMIT = 256


DESCRIPTION_LIMIT = 4096


FIELD_NAME_LIMIT = 256


FIELD_VALUE_LIMIT = 1024


FOOTER_LIMIT = 2048


MAX_FIELDS = 25


# Limite do Discord é 6000 para a soma de título, descrição, campos e rodapé; sobra margem para contagem por pontos de
# código diferente da do Discord.
TOTAL_BUDGET = 5800


MIN_DESCRIPTION = 300


CONTENT_LIMIT = 2000


SHORT_ERROR_LIMIT = 200


VALIDATION_LINE_LIMIT = 300


MISSING_COUNT = "—"


NO_MENTIONS = {"parse": []}


class RunStatus(StrEnum):
    """Estado da execução inteira."""

    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ItemStatus(StrEnum):
    """Estado de uma etapa ou de uma tabela."""

    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"


@dataclass(frozen=True)
class ChecklistItem:
    """Uma linha do checklist de etapas."""

    label: str
    status: ItemStatus


@dataclass(frozen=True)
class Fact:
    """Um campo livre do embed (SCN, workers, slot...)."""

    name: str
    value: str
    inline: bool = True


@dataclass(frozen=True)
class CountSide:
    """Um lado da comparação de contagens.

    :param label: Nome do lado, como ``Oracle (SCN)`` ou ``BigQuery``.
    :param rows: Linhas contadas nesse lado; ``None`` se ainda não se sabe (aparece como ``—``).
    """

    label: str
    rows: int | None


@dataclass(frozen=True)
class ValidationLine:
    """A comparação de contagens de uma tabela entre a origem e o destino.

    :param table: Nome da tabela.
    :param sides: Contagens a comparar, na ordem em que aparecem.
    :param note: Complemento mostrado só quando todas as contagens são iguais (por exemplo, o checksum conferido).
    """

    table: str
    sides: tuple[CountSide, ...]
    note: str = ""

    @property
    def has_counts(self) -> bool:
        """Indica se ao menos um lado já tem contagem."""
        return any(side.rows is not None for side in self.sides)

    @property
    def is_valid(self) -> bool:
        """Indica se todos os lados têm contagem e elas são iguais."""
        rows = {side.rows for side in self.sides}
        return bool(self.sides) and None not in rows and len(rows) == 1


@dataclass(frozen=True)
class TableView:
    """O bloco de uma tabela na descrição.

    :param name: Nome da tabela.
    :param stage: Etapa da tabela, já em texto.
    :param status: Estado da tabela.
    :param fraction: Progresso da tabela de 0 a 1; ``None`` esconde a barra.
    :param detail: Linhas, taxa e volume já formatados; no sucesso, o resumo final (linhas e duração).
    :param eta_seconds: Segundos que faltam para a tabela; ``None`` se não há estimativa.
    """

    name: str
    stage: str
    status: ItemStatus
    fraction: float | None = None
    detail: str = ""
    eta_seconds: float | None = None


@dataclass(frozen=True)
class RunView:
    """Tudo que o embed mostra, sem nenhum I/O.

    :param flow_label: Nome curto do fluxo, como ``Oracle → BigQuery``.
    :param dataset_id: Dataset de destino, que completa o título.
    :param url: Link do flow run na UI do Prefect, se houver.
    :param status: Estado da execução.
    :param stage: Etapa atual (ou a última, no fim).
    :param fraction: Progresso geral de 0 a 1.
    :param elapsed_seconds: Tempo desde o início.
    :param run_name: Nome do flow run, no rodapé.
    :param updated_at: Hora da atualização, com fuso.
    :param checklist: Etapas e seus estados.
    :param tables: Blocos por tabela.
    :param facts: Campos específicos do fluxo.
    :param eta_seconds: Segundos que faltam para o fim; ``None`` esconde a previsão.
    :param error: Mensagem do erro, em caso de falha.
    :param failed_stage: Etapa em que a falha ocorreu.
    :param validation: Comparação de contagens por tabela, mostrada na mensagem final; vazio a esconde.
    """

    flow_label: str
    dataset_id: str
    url: str | None
    status: RunStatus
    stage: str
    fraction: float
    elapsed_seconds: float
    run_name: str
    updated_at: datetime
    checklist: tuple[ChecklistItem, ...] = ()
    tables: tuple[TableView, ...] = ()
    facts: tuple[Fact, ...] = ()
    eta_seconds: float | None = None
    error: str | None = None
    failed_stage: str | None = None
    validation: tuple[ValidationLine, ...] = ()

    @property
    def title(self) -> str:
        """Título do embed: fluxo e dataset."""
        return f"{self.flow_label} · {self.dataset_id}"


STATUS_LINE = {
    RunStatus.RUNNING: "🔄 **EM ANDAMENTO**",
    RunStatus.SUCCESS: "✅ **CONCLUÍDO**",
    RunStatus.FAILED: "❌ **FALHOU**",
    RunStatus.CANCELLED: "⚪ **CANCELADO**",
}


ITEM_ICON = {ItemStatus.PENDING: "⬜", ItemStatus.RUNNING: "🔄", ItemStatus.DONE: "✅", ItemStatus.FAILED: "❌"}
