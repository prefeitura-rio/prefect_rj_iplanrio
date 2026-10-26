"""The calls to the SFMC APIs: the pages of Data Extensions, records and fields."""

from httpx2 import AsyncClient
from pydantic import JsonValue

from pipelines.rj_crm__get_history_data.constants import PAGE_SIZE, SOAP_HEADERS
from pipelines.rj_crm__get_history_data.helpers.sfmc import (
    build_fields_envelope,
    build_list_envelope,
    parse_extensions,
    parse_fields,
)
from pipelines.rj_crm__get_history_data.types import Extension, Field
from utils.records import dig, read_json


async def list_extension_pages(client: AsyncClient, endpoint: str, token: str) -> list[Extension]:
    """Return the Data Extensions of the SOAP API, page by page.

    Raise `RuntimeError` if the API answers with a status that is neither `OK` nor
    `MoreDataAvailable`.
    """
    found: list[Extension] = []
    continue_id: str | None = None

    while True:
        response = await client.post(endpoint, content=build_list_envelope(token, continue_id), headers=SOAP_HEADERS)
        response.raise_for_status()
        status, request_id, page = parse_extensions(response.text)
        found += page

        if status == "MoreDataAvailable":
            continue_id = request_id
        elif status in ("OK", ""):
            return found
        else:
            raise RuntimeError(f"The SOAP API answered with an unexpected status: {status}")


async def read_records(client: AsyncClient, url: str, token: str) -> tuple[list[dict[str, JsonValue]], int]:
    """Return the records of a Data Extension and the number of pages that held them."""
    records: list[dict[str, JsonValue]] = []
    page = 1

    while True:
        response = await client.get(
            url, headers={"Authorization": f"Bearer {token}"}, params={"$page": page, "$pageSize": PAGE_SIZE}
        )
        response.raise_for_status()
        body = read_json(response)
        items = dig(body, "items")
        batch = [item for item in items if isinstance(item, dict)] if isinstance(items, list) else []
        records += batch
        total = dig(body, "count")

        if not batch or len(records) >= (total if isinstance(total, int) else len(batch)):
            return records, page

        page += 1


async def read_fields(client: AsyncClient, endpoint: str, token: str, external_key: str) -> list[Field]:
    """Return the columns of a Data Extension from the SOAP API."""
    response = await client.post(endpoint, content=build_fields_envelope(token, external_key), headers=SOAP_HEADERS)
    response.raise_for_status()

    return parse_fields(response.text)
