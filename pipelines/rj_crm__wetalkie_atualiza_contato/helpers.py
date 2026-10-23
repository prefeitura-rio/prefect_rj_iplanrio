"""Helpers of rj_crm__wetalkie_atualiza_contato."""

from json import dumps

from pydantic import JsonValue

from utils.records import dig
from utils.utils import render_query


def build_query() -> str:
    """Return the query of the ids of the contacts that the table does not have."""
    return render_query(__file__, "missing_contacts")


def build_row(contact_id: str, response: JsonValue) -> dict[str, str] | None:
    """Return the row of a contact from the response of the API, or nothing if it has no data.

    The row has the id and the contact as JSON text.
    """
    item = dig(response, "data", "item")

    if not item:
        return None

    return {"id_contato": contact_id, "json_data": dumps(item)}
