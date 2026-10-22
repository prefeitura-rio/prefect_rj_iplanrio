"""Helpers of rj_crm__webhook_wetalkie_template_status_update."""

from collections.abc import Mapping


def build_message(row: Mapping[str, object]) -> str | None:
    """Return the alert of a change in the status of a template."""
    name = row.get("content_message_template_name")
    event = row.get("content_event")
    reason = row.get("content_reason")
    description = row.get("content_other_info_description")

    message = f"⚠️ Atenção:\n- O template **`{name}`** teve seu status alterado para **`{event}`**"

    lines = []
    if reason and not (event == "APPROVED" and reason == "NONE"):
        lines.append(f"  - Reason: **`{reason}`**")
    if description:
        lines.append(f"  - **`{description}`**")

    if lines:
        message += "\n- Outras informações:\n" + "\n".join(lines)

    return message
