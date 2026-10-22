"""Helpers of rj_crm__webhook_wetalkie_channel_quality_update."""

from collections.abc import Mapping


def build_message(row: Mapping[str, object]) -> str | None:
    """Return the alert of a change in the quality of a phone number."""
    phone = row["content_display_phone_number"]
    event = row["content_event"]
    limit = row["content_current_limit"]

    return (
        f"⚠️ Atenção:\nO número **`{phone}`** sofreu uma alteração do tipo **`{event}`** "
        f"e o novo limite de disparos é: **`{limit}`**"
    )
