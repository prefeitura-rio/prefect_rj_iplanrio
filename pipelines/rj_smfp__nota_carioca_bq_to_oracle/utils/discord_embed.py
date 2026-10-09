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

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.discord_format import (
    clip,
    format_clock,
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
    match view.status:
        case RunStatus.SUCCESS:
            count = len(view.tables)
            text = f"✅ {name} concluído em {took} · {count} {'tabela' if count == 1 else 'tabelas'}{link}"
        case RunStatus.FAILED:
            text = f"❌ {name} falhou na etapa {stage} após {took}: `{short_error(view.error)}`{link}"
        case RunStatus.CANCELLED:
            text = f"⚪ {name} cancelado na etapa {stage} após {took}{link}"
        case RunStatus.RUNNING:
            raise ValueError("A mensagem final só existe depois do término da execução.")
        case unreachable:
            assert_never(unreachable)
    return {"content": clip(text, CONTENT_LIMIT), "allowed_mentions": dict(NO_MENTIONS)}
