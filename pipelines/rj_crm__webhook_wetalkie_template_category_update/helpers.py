"""Helpers of rj_crm__webhook_wetalkie_template_category_update."""

from collections.abc import Mapping


def build_message(row: Mapping[str, object]) -> str | None:
    """Return the alert of a change in the category of a template."""
    name = row["content_message_template_name"]
    previous = row["content_previous_category"]
    new = row["content_new_category"]

    return f"⚠️ Atenção:\nO template **`{name}`** teve a sua categoria atualizada de **`{previous}`** para **`{new}`**"
