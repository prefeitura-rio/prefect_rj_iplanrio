"""Montagem pura do embed de progresso do Discord: estado (``RunView``) entra, ``dict`` do payload sai.

Este arquivo é idêntico nas duas pipelines da Nota Carioca (cada imagem Docker copia só a sua pasta), exceto pelo
caminho do import de ``discord_format``. Não faz HTTP nem lê relógio: quem chama entrega a hora em
``RunView.updated_at``.
Respeita os limites do Discord: descrição 4096, valor de campo 1024, 25 campos e 6000 caracteres no embed todo.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import assert_never

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.discord_format import (
    clip,
    format_clock,
    format_count,
    format_duration,
    format_percent,
    format_short_duration,
    format_stamp,
    progress_bar,
)

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


_STATUS_LINE = {
    RunStatus.RUNNING: "🔄 **EM ANDAMENTO**",
    RunStatus.SUCCESS: "✅ **CONCLUÍDO**",
    RunStatus.FAILED: "❌ **FALHOU**",
    RunStatus.CANCELLED: "⚪ **CANCELADO**",
}
_ITEM_ICON = {ItemStatus.PENDING: "⬜", ItemStatus.RUNNING: "🔄", ItemStatus.DONE: "✅", ItemStatus.FAILED: "❌"}


def status_color(status: RunStatus) -> int:
    """Cor do embed para o estado."""
    match status:
        case RunStatus.RUNNING:
            return COLOR_RUNNING
        case RunStatus.SUCCESS:
            return COLOR_SUCCESS
        case RunStatus.FAILED:
            return COLOR_FAILURE
        case RunStatus.CANCELLED:
            return COLOR_CANCELLED
        case unreachable:
            assert_never(unreachable)


def table_block(table: TableView, status: RunStatus) -> str:
    """Monta o bloco de uma tabela: nome, etapa, barra, detalhes e ETA (só uma linha no sucesso)."""
    icon = _ITEM_ICON[table.status]
    if status is RunStatus.SUCCESS:
        return f"{icon} **{table.name}**" + (f" — {table.detail}" if table.detail else "")
    lines = [f"{icon} **{table.name}** · {table.stage}"]
    if table.fraction is not None:
        lines.append(f"`{progress_bar(table.fraction)}` {format_percent(table.fraction)}")
    meta = [table.detail] if table.detail else []
    if table.eta_seconds is not None and table.status is ItemStatus.RUNNING:
        meta.append(f"faltam ~{format_duration(table.eta_seconds)}")
    if meta:
        lines.append(" · ".join(meta))
    return "\n".join(lines)


def _count_text(side: CountSide) -> str:
    return f"{side.label} {format_count(side.rows) if side.rows is not None else MISSING_COUNT}"


def _count_separator(left: CountSide, right: CountSide) -> str:
    if left.rows is None or right.rows is None:
        return " · "
    return " = " if left.rows == right.rows else " ≠ "


def format_validation_line(line: ValidationLine) -> str:
    """Formata a comparação de uma tabela em uma linha.

    ✅ quando todas as contagens existem e são iguais; ⬜ se nenhuma foi medida ainda; ❌ nos demais casos (diferem ou
    falta um lado, mostrado como ``—``). Entre dois lados: ``=`` se iguais, ``≠`` se diferem, ``·`` se falta um.

    :param line: Contagens da tabela.
    :returns: Por exemplo ``✅ DPS · Oracle (SCN) 10 = BigQuery 10 · Σ VALOR iguais``.
    """
    icon = "✅" if line.is_valid else "❌" if line.has_counts else "⬜"
    chain = _count_chain(line.sides)
    note = f" · {line.note}" if line.note and line.is_valid else ""
    return clip(f"{icon} {line.table} · {chain}{note}", VALIDATION_LINE_LIMIT)


def count_validated(lines: tuple[ValidationLine, ...]) -> int:
    """Conta as tabelas cujas contagens são todas iguais."""
    return sum(line.is_valid for line in lines)


def _count_chain(sides: tuple[CountSide, ...]) -> str:
    if not sides:
        return ""
    parts = [_count_text(sides[0])]
    for left, right in zip(sides, sides[1:], strict=False):
        parts.append(_count_separator(left, right) + _count_text(right))
    return "".join(parts)


def validation_summary(view: RunView) -> str:
    """Resumo da validação para a mensagem final avulsa; vazio se não há linhas de validação.

    :param view: Estado final.
    :returns: Por exemplo `` · 3/3 tabelas validadas (linhas iguais na origem e no destino)``.
    """
    total = len(view.validation)
    if not total:
        return ""
    valid = count_validated(view.validation)
    text = f" · {valid}/{total} {'tabela validada' if total == 1 else 'tabelas validadas'}"
    return text + (" (linhas iguais na origem e no destino)" if valid == total else "")


def build_description(view: RunView) -> str:
    """Monta a descrição: estado, etapa, barra geral e um bloco por tabela."""
    parts = [
        f"{_STATUS_LINE[view.status]} · {view.stage}",
        f"`{progress_bar(view.fraction)}` {format_percent(view.fraction)}",
    ]
    blocks = "\n\n".join(table_block(table, view.status) for table in view.tables)
    return clip("\n".join(parts) + (f"\n\n{blocks}" if blocks else ""), DESCRIPTION_LIMIT)


def build_fields(view: RunView) -> list[dict[str, object]]:
    """Monta os campos: tempos, etapa, checklist, fatos do fluxo e, na falha, o erro."""
    fields: list[tuple[str, str, bool]] = [("⏱️ Decorrido", format_duration(view.elapsed_seconds), True)]
    if view.eta_seconds is not None and view.status is RunStatus.RUNNING:
        eta_at = view.updated_at + timedelta(seconds=view.eta_seconds)
        fields.append(("🏁 Previsão de término", f"~{format_clock(eta_at)} (BRT)", True))
    stage = view.stage
    if view.failed_stage and view.status is RunStatus.FAILED:
        stage = f"Falhou em {view.failed_stage}"
    elif view.failed_stage and view.status is RunStatus.CANCELLED:
        stage = f"Cancelado em {view.failed_stage}"
    fields.append(("🧭 Etapa atual", stage, True))
    if view.checklist:
        fields.append(
            ("Etapas", "\n".join(f"{_ITEM_ICON[item.status]} {item.label}" for item in view.checklist), False)
        )
    if view.validation:
        fields.append(("🔎 Validação", "\n".join(format_validation_line(line) for line in view.validation), False))
    fields.extend((fact.name, fact.value, fact.inline) for fact in view.facts)
    if view.error:
        fields.append(("❌ Erro", f"```\n{clip(view.error.replace('```', "'''"), FIELD_VALUE_LIMIT - 8)}\n```", False))
    return [
        {"name": clip(name, FIELD_NAME_LIMIT), "value": clip(value, FIELD_VALUE_LIMIT) or "—", "inline": inline}
        for name, value, inline in fields[:MAX_FIELDS]
    ]


def _embed_size(embed: dict[str, object], fields: list[dict[str, object]]) -> int:
    footer = embed["footer"]
    footer_text = footer["text"] if isinstance(footer, dict) else ""
    field_chars = sum(len(str(field["name"])) + len(str(field["value"])) for field in fields)
    return len(str(embed["title"])) + len(str(embed["description"])) + len(str(footer_text)) + field_chars


def build_payload(view: RunView) -> dict[str, object]:
    """Monta o corpo do POST/PATCH do webhook, dentro dos limites do Discord.

    :param view: Estado a mostrar.
    :returns: ``{"embeds": [...], "allowed_mentions": {"parse": []}}``; sem menções.
    """
    fields = build_fields(view)
    embed: dict[str, object] = {
        "title": clip(view.title, TITLE_LIMIT),
        "color": status_color(view.status),
        "description": build_description(view),
        "fields": fields,
        "footer": {"text": clip(f"atualizado em {format_stamp(view.updated_at)} · {view.run_name}", FOOTER_LIMIT)},
    }
    if view.url:
        embed["url"] = view.url
    other = _embed_size({**embed, "description": ""}, fields)
    embed["description"] = clip(str(embed["description"]), max(TOTAL_BUDGET - other, MIN_DESCRIPTION))
    while fields and _embed_size(embed, fields) > TOTAL_BUDGET:
        fields.pop()
    return {"embeds": [embed], "allowed_mentions": dict(NO_MENTIONS)}


def short_error(error: str | None) -> str:
    """Resume o erro em uma linha curta, sem crases, para a mensagem final."""
    text = " ".join((error or "sem detalhes").replace("`", "'").split())
    return clip(text, SHORT_ERROR_LIMIT)


def build_announcement(view: RunView) -> dict[str, object]:
    """Monta a mensagem final avulsa, de uma linha (uma mensagem nova notifica; a edição não).

    :param view: Estado final da execução.
    :returns: ``{"content": ..., "allowed_mentions": {"parse": []}}``.
    :raises ValueError: Se a execução ainda estiver em andamento.
    """
    name = f"**{view.flow_label}** (`{view.dataset_id}`)"
    link = f" · <{view.url}>" if view.url else ""
    took = format_short_duration(view.elapsed_seconds)
    stage = f"**{view.failed_stage or view.stage}**"
    summary = validation_summary(view)
    match view.status:
        case RunStatus.SUCCESS:
            count = len(view.tables)
            tables = summary or f" · {count} {'tabela' if count == 1 else 'tabelas'}"
            text = f"✅ {name} concluído em {took}{tables}{link}"
        case RunStatus.FAILED:
            text = f"❌ {name} falhou na etapa {stage} após {took}: `{short_error(view.error)}`{summary}{link}"
        case RunStatus.CANCELLED:
            text = f"⚪ {name} cancelado na etapa {stage} após {took}{link}"
        case RunStatus.RUNNING:
            raise ValueError("A mensagem final só existe depois do término da execução.")
        case unreachable:
            assert_never(unreachable)
    return {"content": clip(text, CONTENT_LIMIT), "allowed_mentions": dict(NO_MENTIONS)}
