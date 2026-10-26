"""The Salesforce APIs: the login, Data Cloud and the CRM REST API."""

from collections.abc import Sequence

from httpx2 import AsyncClient
from pydantic import JsonValue, TypeAdapter
from toolkit.telemetry import logger

from pipelines.rj_crm__salesforce_datacloud.constants import (
    CRM_QUERY_PATH,
    CRM_TIMEOUT,
    LOGIN_TIMEOUT,
    MAX_ROWS,
    PAGE_SIZE,
    QUERY_PATH,
    TIMEOUT,
    WORKLOAD,
)
from pipelines.rj_crm__salesforce_datacloud.helpers.transform import clean_records
from pipelines.rj_crm__salesforce_datacloud.types import Row, Session
from utils.records import dig

JSON: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)


def as_rows(value: JsonValue) -> list[Sequence[JsonValue]]:
    """Return a value as the list of lists that Data Cloud sends as `data`, or an empty list."""
    return [row for row in value if isinstance(row, list)] if isinstance(value, list) else []


async def log_in(instance_url: str, client_id: str, client_secret: str, dataspace: str) -> Session:
    """Log in to Salesforce with the client credentials of the Connected App `DataCloud_Integration`.

    The login serves Data Cloud and the CRM REST API. Raise `HTTPStatusError` if
    Salesforce refuses it.
    """
    async with AsyncClient(timeout=LOGIN_TIMEOUT) as client:
        response = await client.post(
            f"{instance_url}/services/oauth2/token",
            data={"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret},
        )
        response.raise_for_status()

    body = JSON.validate_json(response.content)
    token = dig(body, "access_token")

    if not isinstance(token, str):
        raise ValueError("The login of Salesforce has no access_token")

    url = dig(body, "instance_url")

    return Session(
        access_token=token,
        instance_url=(url if isinstance(url, str) else instance_url).rstrip("/"),
        dataspace=dataspace,
    )


async def query_data_cloud(
    session: Session, sql: str, order_by: str, name: str
) -> tuple[list[str], list[Sequence[JsonValue]]]:
    """Run a query in Data Cloud and return the names of its columns and all its rows.

    The API has no continuation, so each page is the query again with `ORDER BY`,
    `LIMIT` and `OFFSET`. `sql` must have none of them. The pages end when one comes
    with fewer rows than asked. Raise `HTTPStatusError` for an error, after logging the
    answer of Salesforce.
    """
    url = f"{session['instance_url']}{QUERY_PATH}?dataspace={session['dataspace']}&workloadName={WORKLOAD}"
    headers = {"Authorization": f"Bearer {session['access_token']}", "Content-Type": "application/json"}
    names: list[str] = []
    rows: list[Sequence[JsonValue]] = []
    offset = 0

    async with AsyncClient(timeout=TIMEOUT) as client:
        while True:
            page_sql = f"{sql.rstrip().rstrip(';')} ORDER BY {order_by} LIMIT {PAGE_SIZE} OFFSET {offset}"
            response = await client.post(url, headers=headers, json={"sql": page_sql})

            if not response.is_success:
                logger.error(
                    "Data Cloud error: table=%s status=%d answer=%s", name, response.status_code, response.text[:1000]
                )

            response.raise_for_status()
            body = JSON.validate_json(response.content)

            if not offset:
                metadata = dig(body, "metadata")
                names = [str(dig(column, "name")) for column in metadata] if isinstance(metadata, list) else []

            page = as_rows(dig(body, "data"))
            rows += page

            if len(page) < PAGE_SIZE:
                break

            offset += PAGE_SIZE

            if offset >= MAX_ROWS:
                logger.warning("Pagination stopped at the safety limit: table=%s rows=%d", name, MAX_ROWS)
                break

    return names, rows


async def query_crm(session: Session, soql: str) -> list[Row]:
    """Run a SOQL query in the CRM REST API and return all its records, following `nextRecordsUrl`.

    Raise `HTTPStatusError` for an error.
    """
    headers = {"Authorization": f"Bearer {session['access_token']}"}
    records: list[dict[str, JsonValue]] = []

    async with AsyncClient(timeout=CRM_TIMEOUT, headers=headers) as client:
        response = await client.get(f"{session['instance_url']}{CRM_QUERY_PATH}", params={"q": soql.strip()})
        response.raise_for_status()
        body = JSON.validate_json(response.content)

        while True:
            found = dig(body, "records")
            records += [record for record in found if isinstance(record, dict)] if isinstance(found, list) else []

            if dig(body, "done") or not isinstance(dig(body, "nextRecordsUrl"), str):
                break

            response = await client.get(f"{session['instance_url']}{dig(body, 'nextRecordsUrl')}")
            response.raise_for_status()
            body = JSON.validate_json(response.content)

    return clean_records(records)
