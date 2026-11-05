"""Discord notification message builders (pure functions, no I/O)."""

from prefect.client.schemas.objects import FlowRun

LIMIT = 1900


def truncate(text: str) -> str:
    """Trim `text` to fit Discord's 2000-character message limit."""
    return text if len(text) <= LIMIT else text[: LIMIT - 3] + "..."


def build_table_failure_message(table: str, group: str, window: str, error: str) -> str:
    """Build a failure notification for a single table."""
    # Slice from the end so the actual exception message is preserved.
    return truncate(f"\u274c **{table}** ({group}) failed\nwindow: {window}\n```{error[-900:]}```")


def build_flow_failure_message(flow_run: FlowRun) -> str:
    """Build a notification for a flow that aborted entirely."""
    state_name = flow_run.state.name if flow_run.state else "?"
    return (
        f"\U0001f6d1 **czrm-salesforce-datacloud** aborted\n"
        f"run: `{flow_run.name}`\n"
        f"state: `{state_name}`"
    )


def build_summary_message(
    label: str,
    results: dict[str, dict[str, int]],
    failures: list[str],
    minutes: float,
) -> str:
    """Build a run-summary notification for cold modes and initial loads."""
    lines = [f"\u2705 **czrm-salesforce-datacloud** \u2014 {label} ({minutes:.1f} min)"]
    for group, tables in results.items():
        detail = ", ".join(f"{name}: {qty}" for name, qty in tables.items())
        lines.append(f"- **{group}**: {detail}")
    if failures:
        lines.append(f"- \u26a0\ufe0f failed: {', '.join(failures)}")
    return truncate("\n".join(lines))
