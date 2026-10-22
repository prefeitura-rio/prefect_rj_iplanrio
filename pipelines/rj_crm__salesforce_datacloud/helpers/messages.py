"""The messages of Discord."""

from collections.abc import Mapping, Sequence

from pipelines.rj_crm__salesforce_datacloud.constants import DISCORD_LIMIT, DISCORD_NAME

ERROR_LIMIT = 1500


def cut(message: str) -> str:
    """Return the message cut to the limit of a webhook, with a note when it is cut."""
    if len(message) <= DISCORD_LIMIT:
        return message

    return message[: DISCORD_LIMIT - 20].rstrip() + "\n… (cortado)"


def build_feed_failure(name: str, group: str, window: str, error: str) -> str:
    """Return the message of a feed that failed while the run went on."""
    lines = [
        f"⚠️ **{DISCORD_NAME}: tabela `{name}` falhou** ({group}) — o resto do flow seguiu",
        "",
        f"**Janela:** {window}",
        f"**Erro:** ```{error[:ERROR_LIMIT]}```",
    ]

    return cut("\n".join(lines))


def build_run_failure(flow_name: str, flow_run: str, error: str | None) -> str:
    """Return the message of a run that stopped."""
    lines = [
        f"🚨 **{flow_name}: FALHOU**",
        "",
        f"**Flow run:** {flow_run}",
        f"**Erro:** ```{(error or 'sem mensagem')[:ERROR_LIMIT]}```",
    ]

    return cut("\n".join(lines))


def build_summary(label: str, rows: Mapping[str, Mapping[str, int]], failed: Sequence[str], minutes: float) -> str:
    """Return the message of the end of a run: the rows of each feed, by group."""
    lines = [f"{'⚠️' if failed else '✅'} **{DISCORD_NAME}: concluído** — {label}", ""]

    for group, feeds in rows.items():
        lines.append(f"**{group}**")
        lines += [f"• `{name}`: {count:,} linhas".replace(",", ".") for name, count in feeds.items()]

    if failed:
        lines += ["", f"❌ **Falharam:** {', '.join(f'`{name}`' for name in failed)}"]

    lines += ["", f"⏱️ {minutes:.1f} min"]

    return cut("\n".join(lines))
