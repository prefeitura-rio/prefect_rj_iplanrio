"""Helpers of rj_crm__webhook_wetalkie_template_quality_update."""

from collections.abc import Mapping

QUALITY_LABELS = {
    "GREEN": "🟢 Alta qualidade",
    "YELLOW": "🟡 Média qualidade",
    "RED": "🔴 Baixa qualidade",
    "UNKNOWN": "❓ Desconhecido",
}


def build_message(row: Mapping[str, object]) -> str | None:
    """Return the alert of a drop in the quality of a template, or `None` if it is not a drop."""
    name = row["content_message_template_name"]
    new = row.get("content_new_quality_score", "UNKNOWN")

    if new not in ("YELLOW", "RED"):
        return None

    previous_label = QUALITY_LABELS.get(str(row.get("content_previous_quality_score", "UNKNOWN")), "❓ Desconhecido")
    new_label = QUALITY_LABELS.get(str(new), "❓ Desconhecido")

    return (
        f"⚠️ Atenção:\nO template **`{name}`** teve a sua qualidade atualizada "
        f"de **`{previous_label}`** para **`{new_label}`**"
    )
