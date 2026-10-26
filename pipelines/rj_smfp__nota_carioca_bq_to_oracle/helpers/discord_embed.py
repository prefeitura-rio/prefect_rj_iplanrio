"""Build the Discord progress embed from a `RunView`, as the payload `dict`.

This file is the same in both Nota Carioca pipelines (each Docker image copies only
its own folder), except for the import path of `discord_format`. It makes no HTTP
call and reads no clock: the caller gives the time in `RunView.updated_at`. It keeps
the limits of Discord: 4096 for the description, 1024 for the value of a field, 25
fields, and 6000 characters in the whole embed.
"""

from datetime import timedelta
from itertools import pairwise

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.discord_format import (
    clip,
    format_clock,
    format_count,
    format_duration,
    format_percent,
    format_short_duration,
    format_stamp,
    progress_bar,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.discord_view import (
    COLOR_CANCELLED,
    COLOR_FAILURE,
    COLOR_RUNNING,
    COLOR_SUCCESS,
    CONTENT_LIMIT,
    DESCRIPTION_LIMIT,
    FIELD_NAME_LIMIT,
    FIELD_VALUE_LIMIT,
    FOOTER_LIMIT,
    ITEM_ICON,
    MAX_FIELDS,
    MIN_DESCRIPTION,
    MISSING_COUNT,
    NO_MENTIONS,
    SHORT_ERROR_LIMIT,
    STATUS_LINE,
    TITLE_LIMIT,
    TOTAL_BUDGET,
    VALIDATION_LINE_LIMIT,
    CountSide,
    ItemStatus,
    RunStatus,
    RunView,
    TableView,
    ValidationLine,
)


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


def table_block(table: TableView, status: RunStatus) -> str:
    """Build the block of a table: name, stage, bar, details and ETA.

    On success it is one line only.
    """
    icon = ITEM_ICON[table.status]
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


def count_text(side: CountSide) -> str:
    """Return the label of a side and its count."""
    return f"{side.label} {format_count(side.rows) if side.rows is not None else MISSING_COUNT}"


def count_separator(left: CountSide, right: CountSide) -> str:
    """Return `=` if the two counts are equal, `≠` if not, and `·` if one is missing."""
    if left.rows is None or right.rows is None:
        return " · "
    return " = " if left.rows == right.rows else " ≠ "


def format_validation_line(line: ValidationLine) -> str:
    """Format the comparison of a table as one line.

    ✅ when all the counts exist and are equal, ⬜ if none was measured yet, and ❌ in
    the other cases (they differ, or a side is missing and shown as `—`). Between two
    sides: `=` if equal, `≠` if different, `·` if one is missing.

    ```text
    ✅ DPS · Oracle (SCN) 10 = BigQuery 10 · Σ VALOR iguais
    ```
    """
    icon = "✅" if line.is_valid else "❌" if line.has_counts else "⬜"
    chain = count_chain(line.sides)
    note = f" · {line.note}" if line.note and line.is_valid else ""
    return clip(f"{icon} {line.table} · {chain}{note}", VALIDATION_LINE_LIMIT)


def count_validated(lines: tuple[ValidationLine, ...]) -> int:
    """Conta as tabelas cujas contagens são todas iguais."""
    return sum(line.is_valid for line in lines)


def count_chain(sides: tuple[CountSide, ...]) -> str:
    """Return the sides as one text, with a separator between each pair."""
    if not sides:
        return ""
    parts = [count_text(sides[0])]
    for left, right in pairwise(sides):
        parts.append(count_separator(left, right) + count_text(right))
    return "".join(parts)


def validation_summary(view: RunView) -> str:
    """Summarize the validation for the final message, or give nothing without lines.

    ```text
     · 3/3 tabelas validadas (linhas iguais na origem e no destino)
    ```
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
        f"{STATUS_LINE[view.status]} · {view.stage}",
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
        fields.append(("Etapas", "\n".join(f"{ITEM_ICON[item.status]} {item.label}" for item in view.checklist), False))
    if view.validation:
        fields.append(("🔎 Validação", "\n".join(format_validation_line(line) for line in view.validation), False))
    fields.extend((fact.name, fact.value, fact.inline) for fact in view.facts)
    if view.error:
        fields.append(("❌ Erro", f"```\n{clip(view.error.replace('```', "'''"), FIELD_VALUE_LIMIT - 8)}\n```", False))
    return [
        {"name": clip(name, FIELD_NAME_LIMIT), "value": clip(value, FIELD_VALUE_LIMIT) or "—", "inline": inline}
        for name, value, inline in fields[:MAX_FIELDS]
    ]


def embed_size(embed: dict[str, object], fields: list[dict[str, object]]) -> int:
    """Return the characters that Discord counts in an embed."""
    footer = embed["footer"]
    footer_text = footer["text"] if isinstance(footer, dict) else ""
    field_chars = sum(len(str(field["name"])) + len(str(field["value"])) for field in fields)
    return len(str(embed["title"])) + len(str(embed["description"])) + len(str(footer_text)) + field_chars


def build_payload(view: RunView) -> dict[str, object]:
    """Build the body of the POST or PATCH of the webhook, inside the limits of Discord.

    Return `{"embeds": [...], "allowed_mentions": {"parse": []}}`, which has no mention.
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
    other = embed_size({**embed, "description": ""}, fields)
    embed["description"] = clip(str(embed["description"]), max(TOTAL_BUDGET - other, MIN_DESCRIPTION))
    while fields and embed_size(embed, fields) > TOTAL_BUDGET:
        fields.pop()
    return {"embeds": [embed], "allowed_mentions": dict(NO_MENTIONS)}


def short_error(error: str | None) -> str:
    """Shorten the error to one short line, with no backticks, for the final message."""
    text = " ".join((error or "sem detalhes").replace("`", "'").split())
    return clip(text, SHORT_ERROR_LIMIT)


def build_announcement(view: RunView) -> dict[str, object]:
    """Build the final one-line message.

    A new message notifies, and an edit does not.

    Return `{"content": ..., "allowed_mentions": {"parse": []}}`. Raise `ValueError` if
    the run is still going.
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
    return {"content": clip(text, CONTENT_LIMIT), "allowed_mentions": dict(NO_MENTIONS)}
